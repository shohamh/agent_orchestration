"""Cards that start later or repeat. The flow host stores them; the board starts them.

An engine-neutral record, so a schedule behaves the same on Hatchet, Prefect and Trigger.dev. A
record is the card's create request plus when it runs: `at` (one time) and/or `cron` (five-field
cron evaluated in the user's time zone). Missed runs are not made up: a board that was down starts
a due schedule once and moves on to the next future time.
"""
import json
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone

from .restart import atomic_json

FIELDS = ((0, 59), (0, 23), (1, 31), (1, 12), (0, 7))
SEARCH_YEARS = 5
MAX_SCHEDULES = 500
OVERLAP = ('skip', 'start')  # what a repeat does while its previous run is active
_LOCK = threading.Lock()


def _field(text, low, high):
    values = set()
    for part in text.split(','):
        match = re.fullmatch(r'(\*|\d+(?:-\d+)?)(?:/(\d+))?', part)
        if not match: raise ValueError(f'Invalid cron field {text!r}')
        span, step = match[1], int(match[2] or 1)
        if step < 1: raise ValueError(f'Invalid cron step in {text!r}')
        if span == '*': start, end = low, high
        else:
            start, _, end = span.partition('-')
            start = int(start)
            # `5/15` means from 5 to the end of the range, as in other cron implementations.
            end = int(end) if end else (high if match[2] else start)
        if not low <= start <= end <= high: raise ValueError(f'Cron value out of range in {text!r} ({low}-{high})')
        values.update(range(start, end + 1, step))
    return values


def parse_cron(text):
    if not isinstance(text, str): raise ValueError('cron must be text')
    parts = text.split()
    if len(parts) != 5: raise ValueError('cron needs five fields: minute hour day month weekday')
    minute, hour, day, month, weekday = (_field(p, *FIELDS[i]) for i, p in enumerate(parts))
    return {'minute': minute, 'hour': hour, 'day': day, 'month': month, 'weekday': {d % 7 for d in weekday},
            'any_day': parts[2] == '*', 'any_weekday': parts[4] == '*'}


def _day_matches(cron, local):
    if local.month not in cron['month']: return False
    day, weekday = local.day in cron['day'], local.isoweekday() % 7 in cron['weekday']
    # Standard cron: when both day fields are restricted, either one matching is enough.
    if cron['any_day'] or cron['any_weekday']: return day and weekday
    return day or weekday


def zone_for(name, offset_minutes=None):
    """The IANA zone, or the browser's current offset where this machine has no zone data."""
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    if name:
        try: return ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError, OSError): pass
    return timezone(timedelta(minutes=offset_minutes or 0))


def next_after(cron, after, zone):
    """The first matching minute after `after` (aware). Steps in UTC so a daylight-saving change
    neither loops nor needs special cases: a local time that does not exist that day is skipped."""
    moment = after.astimezone(timezone.utc).replace(second=0, microsecond=0) + timedelta(minutes=1)
    limit = moment + timedelta(days=366 * SEARCH_YEARS)
    while moment < limit:
        local = moment.astimezone(zone)
        if not _day_matches(cron, local):
            moment += timedelta(minutes=1440 - local.hour * 60 - local.minute)
        elif local.hour not in cron['hour']:
            moment += timedelta(minutes=60 - local.minute)
        elif local.minute in cron['minute']:
            return moment
        else:
            moment += timedelta(minutes=1)
    raise ValueError(f'This cron never runs within {SEARCH_YEARS} years')


def _iso(moment):
    return moment.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


def _parse_time(text):
    if not isinstance(text, str): raise ValueError('at must be an ISO 8601 time')
    try: moment = datetime.fromisoformat(text.replace('Z', '+00:00'))
    except ValueError: raise ValueError('at must be an ISO 8601 time') from None
    if moment.tzinfo is None: raise ValueError('at must include a time zone')
    return moment


def _timing(spec):
    unknown = set(spec) - {'at', 'cron', 'timezone', 'utcOffset', 'summary', 'overlap'}
    if unknown: raise ValueError(f"Unknown schedule fields: {', '.join(sorted(unknown))}")
    if spec.get('overlap', 'skip') not in OVERLAP: raise ValueError('overlap must be skip or start')
    at, cron = spec.get('at'), spec.get('cron')
    if not at and not cron: raise ValueError('A schedule needs a time, a recurrence or both')
    zone_name, offset = spec.get('timezone'), spec.get('utcOffset')
    if zone_name is not None and (not isinstance(zone_name, str) or len(zone_name) > 100): raise ValueError('Invalid time zone')
    if offset is not None and (type(offset) is not int or abs(offset) > 16 * 60): raise ValueError('Invalid UTC offset')
    return {'at': _iso(_parse_time(at)) if at else None, 'cron': ' '.join(cron.split()) if cron else None,
            'timezone': zone_name, 'utcOffset': offset, 'parsed': parse_cron(cron) if cron else None,
            'zone': zone_for(zone_name, offset)}


def upcoming(spec, count=3, now=None):
    """The next start times of a schedule that is being composed (the form's preview)."""
    timing, now = _timing(spec), now or datetime.now(timezone.utc)
    result = []
    moment = now
    if timing['at']:
        first = _parse_time(timing['at'])
        if first <= now: raise ValueError('Choose a time in the future')
        result.append(first)
        moment = first
    while timing['parsed'] and len(result) < count:
        moment = next_after(timing['parsed'], moment, timing['zone'])
        result.append(moment)
    return [_iso(m) for m in result]


class Schedules:
    def __init__(self, path):
        self.path = path

    def _read(self):
        try: data = json.loads(self.path.read_text(encoding='utf-8'))
        except FileNotFoundError: return []
        return data if isinstance(data, list) else []

    def list(self, system=None, board=None):
        with _LOCK: items = self._read()
        return [s for s in items if (system is None or s['system'] == system) and (board is None or s['board'] == board)]

    def referenced_uploads(self):
        with _LOCK: items = self._read()
        return {f['id'] for s in items for f in ((s['request'].get('context') or {}).get('files') or []) if isinstance(f, dict) and f.get('id')}

    def create(self, body, now=None):
        now = now or datetime.now(timezone.utc)
        for key in ('system', 'board', 'title'):
            if not isinstance(body.get(key), str) or not body[key].strip() or len(body[key]) > 300: raise ValueError(f'schedule {key} must be text')
        if not isinstance(body.get('request'), dict): raise ValueError('schedule request must be an object')
        when = body.get('schedule')
        if not isinstance(when, dict): raise ValueError('schedule must be an object')
        timing = _timing(when)
        summary = when.get('summary') or ''
        if not isinstance(summary, str) or len(summary) > 300: raise ValueError('Invalid schedule summary')
        if timing['at']:
            first = _parse_time(timing['at'])
            if first <= now: raise ValueError('Choose a time in the future')
        else:
            first = next_after(timing['parsed'], now, timing['zone'])
        item = {'id': uuid.uuid4().hex, 'system': body['system'], 'board': body['board'], 'title': body['title'].strip(),
                'kind': str(body.get('kind') or ''), 'request': body['request'], 'at': timing['at'], 'cron': timing['cron'],
                'timezone': timing['timezone'], 'utcOffset': timing['utcOffset'], 'summary': summary, 'overlap': when.get('overlap', 'skip'), 'next_at': _iso(first),
                'enabled': True, 'created_at': _iso(now), 'last_at': None, 'last_run': body.get('lastRun'), 'last_error': None, 'last_skip': None}
        with _LOCK:
            items = self._read()
            if len(items) >= MAX_SCHEDULES: raise ValueError(f'At most {MAX_SCHEDULES} schedules are allowed; delete some first')
            atomic_json(self.path, items + [item])
        return item

    def _update(self, schedule_id, change):
        with _LOCK:
            items = self._read()
            item = next((s for s in items if s['id'] == schedule_id), None)
            if item is None: raise LookupError('That schedule no longer exists')
            kept = change(item)
            atomic_json(self.path, items if kept is not False else [s for s in items if s['id'] != schedule_id])
        return item

    def get(self, schedule_id):
        return self._update(schedule_id, lambda item: None)

    def delete(self, schedule_id):
        self._update(schedule_id, lambda item: False)

    def set_enabled(self, schedule_id, enabled, now=None):
        now = now or datetime.now(timezone.utc)
        def change(item):
            item['enabled'] = bool(enabled)
            if not enabled: return
            item['last_error'] = None
            # Resuming never starts what came due while it was paused.
            if item['cron']:
                item['next_at'] = _iso(next_after(parse_cron(item['cron']), now, zone_for(item['timezone'], item['utcOffset'])))
            elif not item['next_at'] or _parse_time(item['next_at']) <= now:
                raise ValueError('This one-time schedule has passed; start it now or delete it')
        return self._update(schedule_id, change)

    def claim(self, systems, now=None):
        """Atomically take the due schedules of these backends and move them to their next time."""
        now, claimed = now or datetime.now(timezone.utc), []
        with _LOCK:
            items = self._read()
            for item in items:
                if not item['enabled'] or item['system'] not in systems or not item['next_at'] or _parse_time(item['next_at']) > now:
                    continue
                claimed.append(json.loads(json.dumps(item)))
                item['last_at'] = _iso(now)
                if item['cron']:
                    item['next_at'] = _iso(next_after(parse_cron(item['cron']), now, zone_for(item['timezone'], item['utcOffset'])))
                else:
                    item['next_at'] = None
            if claimed: atomic_json(self.path, items)
        return claimed

    def record(self, schedule_id, run=None, error=None, skipped=None, now=None):
        """A start's outcome. A one-time schedule that started is finished: its card is on the board."""
        now = now or datetime.now(timezone.utc)
        def change(item):
            item['last_at'] = _iso(now)
            item['last_error'] = str(error)[:1000] if error else None
            item['last_skip'] = str(skipped)[:300] if skipped else None
            if run: item['last_run'] = run
            if item['cron']: return None
            if run: return False
            if error: item['enabled'] = False  # stays visible with its error until it is handled
            return None
        try: return self._update(schedule_id, change)
        except LookupError: return None  # deleted while it was starting
