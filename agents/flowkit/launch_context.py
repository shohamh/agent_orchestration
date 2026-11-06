"""Per-launch input of a card or flow run: uploaded files, git repositories and Confluence page trees.

A flow's `environment` is part of its definition and opens an interactive setup step. This is the
other way in: what the person starting a run attaches in New card. It never waits for a human, and a
source that cannot be prepared fails the run with its error.

The flow host keeps uploads in the workspaces volume its coordinators share. Repositories and
Confluence trees are prepared on an execution worker, once per run, with that worker's credentials.
Every step of the run then receives the same files.
"""
import base64
import json
import os
import re
import shutil
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from agentdriver.events import MAX_OUTPUT, Emitter

from .artifacts import MAX_BYTES, validate
from .environment import configuration, prepare_sources

UPLOAD_RETENTION_SECONDS = 7 * 86400
MAX_SOURCES = 50
GIT_URL = re.compile(r'(?:https?|ssh)://\S+|[\w.-]+@[\w.-]+:\S+')
SOURCE_KEYS = ('repositories', 'files', 'confluence')


def uploads_root():
    base = Path(os.environ.get('FLOW_WORKSPACES') or Path(tempfile.gettempdir()) / 'agent-flows')
    return base / '_uploads'


def _upload_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{32}', value): raise ValueError('Invalid upload id')
    return value


def _file_name(name):
    if not isinstance(name, str) or not name or len(name) > 200 or '/' in name or '\\' in name:
        raise ValueError('An upload needs a plain file name of at most 200 characters')
    validate({name: {'data': '', 'mode': 0o644}})
    if name.startswith('.flow-'): raise ValueError('That file name is reserved')
    return name


def save_upload(name, data, keep=()):
    """Store one uploaded file; returns {id, name, size}. Unused old uploads are removed here."""
    name = _file_name(name)
    if not isinstance(data, str): raise ValueError('data must be base64 text')
    try: content = base64.b64decode(data, validate=True)
    except ValueError: raise ValueError('data must be base64 text') from None
    if len(content) > MAX_BYTES: raise ValueError('A file can be at most 64 MiB (the transfer limit of a run)')
    prune_uploads(keep)
    upload = uuid.uuid4().hex
    directory = uploads_root() / upload
    directory.mkdir(parents=True)
    (directory / name).write_bytes(content)
    return {'id': upload, 'name': name, 'size': len(content)}


def delete_upload(upload):
    shutil.rmtree(uploads_root() / _upload_id(upload), ignore_errors=True)


def prune_uploads(keep=(), now=None):
    """A run copies its uploads when it starts, so only a schedule needs an old one (`keep`)."""
    now, root = now or time.time(), uploads_root()
    if not root.is_dir(): return
    for path in root.iterdir():
        try:
            if path.name not in keep and now - path.stat().st_mtime > UPLOAD_RETENTION_SECONDS:
                shutil.rmtree(path, ignore_errors=True)
        except OSError: pass


def _slug(text):
    return re.sub(r'[^A-Za-z0-9._-]+', '-', text).strip('-.') or 'source'


def _repository(item, taken):
    if not isinstance(item, dict) or set(item) - {'url', 'ref'} or not isinstance(item.get('url'), str):
        raise ValueError('A repository needs a url and optionally a ref')
    url, ref = item['url'].strip(), item.get('ref')
    if not GIT_URL.fullmatch(url):
        raise ValueError(f'{url!r} is not a git URL: use https://, ssh:// or git@host:path')
    name = _slug(re.sub(r'\.git$', '', url.rstrip('/').replace(':', '/').rsplit('/', 1)[-1]))
    target, serial = f'repos/{name}', 1
    while target.casefold() in taken:
        serial += 1
        target = f'repos/{name}-{serial}'
    taken.add(target.casefold())
    result = {'url': url, 'target': target, 'shallow': True}
    if ref is not None and ref != '':
        if not isinstance(ref, str) or len(ref) > 200 or not ref.strip() or ref.startswith('-'): raise ValueError('Invalid git ref')
        result['ref'] = ref.strip()
    return result


def parse_confluence_url(url):
    """(base_url, page_id) of a Confluence page link, Cloud or Server."""
    if not isinstance(url, str): raise ValueError('A Confluence page needs a link')
    parts = urlsplit(url.strip())
    if parts.scheme not in ('http', 'https') or not parts.hostname:
        raise ValueError(f'{url!r} is not a Confluence page link')
    base = f'{parts.scheme}://{parts.netloc}'
    pretty = re.match(r'(.*?)/spaces/[^/]+/pages/(\d+)(?:/|$)', parts.path)
    if pretty: return (base + pretty[1], pretty[2])
    page = parse_qs(parts.query).get('pageId', [''])[0]
    if re.fullmatch(r'\d+', page):
        return (base + re.sub(r'/(?:pages/[^/]*|plugins/.*)$', '', parts.path), page)
    raise ValueError('Paste the link of a Confluence page: it must contain /spaces/<space>/pages/<id> or pageId=<id>')


def _confluence(item, taken):
    if not isinstance(item, dict) or set(item) - {'url', 'include_descendants'}:
        raise ValueError('A Confluence source needs a url and optionally include_descendants')
    base, page = parse_confluence_url(item.get('url'))
    target = f'context/confluence/{page}'
    if target in taken: raise ValueError(f'Confluence page {page} is listed twice')
    taken.add(target)
    include = item.get('include_descendants', True)
    if type(include) is not bool: raise ValueError('include_descendants must be true or false')
    return {'base_url': base, 'page_id': page, 'target': target, 'include_descendants': include}


def _upload(item, taken):
    if not isinstance(item, dict) or set(item) - {'id', 'name'}: raise ValueError('An uploaded file needs an id and a name')
    upload, name = _upload_id(item.get('id')), _file_name(item.get('name'))
    if not (uploads_root() / upload / name).is_file():
        raise ValueError(f'The upload {name} is no longer available; attach the file again')
    target = f'context/files/{name}'
    if target.casefold() in taken: raise ValueError(f'Two uploaded files are named {name}')
    taken.add(target.casefold())
    return {'id': upload, 'name': name, 'source': f'upload:{upload}', 'target': target}


def normalize(value):
    """The stored form of what New card sends, with a workspace folder for each source. Returns
    None when nothing was attached."""
    if value is None: return None
    if not isinstance(value, dict) or set(value) - {*SOURCE_KEYS, 'text'}:
        raise ValueError('context contains text, repositories, files and/or confluence')
    if sum(len(value.get(k) or []) for k in SOURCE_KEYS) > MAX_SOURCES: raise ValueError(f'At most {MAX_SOURCES} sources are allowed')
    for key in SOURCE_KEYS:
        if not isinstance(value.get(key) or [], list): raise ValueError(f'{key} must be a list')
    text = value.get('text') or ''
    if not isinstance(text, str) or len(text) > 20000 or '\x00' in text: raise ValueError('Input text must be at most 20000 characters')
    taken = set()
    result = {'repositories': [_repository(i, taken) for i in value.get('repositories') or []],
              'files': [_upload(i, taken) for i in value.get('files') or []],
              'confluence': [_confluence(i, taken) for i in value.get('confluence') or []]}
    configuration({'repositories': result['repositories'], 'confluence': result['confluence'],
                   'files': [{'source': i['source'], 'target': i['target']} for i in result['files']]})
    result = {k: v for k, v in result.items() if v}
    if text.strip(): result['text'] = text
    return result or None


def has_sources(context):
    return bool(context) and any(context.get(k) for k in SOURCE_KEYS)


def remote_sources(context):
    """What an execution worker has to fetch: everything except the uploads the coordinator holds."""
    return {k: context[k] for k in ('repositories', 'confluence') if context.get(k)}


def targets(sources):
    return ', '.join(i['target'] for items in sources.values() for i in items)


def upload_files(context):
    """The uploads as a run's portable file tree."""
    files = {}
    for item in context.get('files') or []:
        path = uploads_root() / _upload_id(item['id']) / _file_name(item['name'])
        try: content = path.read_bytes()
        except OSError: raise ValueError(f"The upload {item['name']} is no longer available; attach the file again") from None
        files[item['target']] = {'data': base64.b64encode(content).decode(), 'mode': 0o644}
    return validate(files)


def note(context):
    """Tells an agent what was attached, since its prompt is written without knowing the folders."""
    if not has_sources(context): return ''
    lines = [f"- {i['target']} (repository {i['url']})" for i in context.get('repositories') or []]
    lines += [f"- {i['target']} (uploaded file)" for i in context.get('files') or []]
    lines += [f"- {i['target']} (Confluence pages as Markdown; start with index.json)" for i in context.get('confluence') or []]
    return 'Attached to this run, in your workspace:\n' + '\n'.join(lines)


def _with_default_credentials(item):
    """A Confluence page attached in the form uses the worker's standard credential variables when set."""
    extra = {}
    for key, name in (('token_env', 'CONFLUENCE_TOKEN'), ('username_env', 'CONFLUENCE_EMAIL')):
        if os.environ.get(name): extra[key] = name
    return {**item, **extra}


async def prepare_on_worker(context, workspace, sink):
    """Fetch the repositories and Confluence trees into `workspace`, or fail with their errors."""
    config = {**context, 'confluence': [_with_default_credentials(i) for i in context.get('confluence') or []]}
    emit = Emitter(sink)
    for kind in ('repositories', 'confluence'):
        for item in config.get(kind) or []:
            await emit('tool', id='context:' + item['target'], name='Prepare ' + kind, summary=item['target'], status='running')
    report = await prepare_sources(config, workspace)
    for item in report:
        await emit('tool', id='context:' + item['source'], name='Prepare ' + item['kind'], summary=item['source'],
                   status='ok' if item['ok'] else 'error', output=item.get('error', 'Ready'))
    failed = [f"{i['source']}: {i.get('error')}" for i in report if not i['ok']]
    if failed: raise RuntimeError('Could not prepare what was attached to this run:\n' + '\n'.join(failed))
    return {'sources': report}


async def prepare_locally(context, workspace, sink):
    """The same for a card that runs on this machine (Trigger.dev, or an existing workspace)."""
    from .artifacts import restore
    restore(upload_files(context), workspace)
    if not remote_sources(context): return
    try:
        await prepare_on_worker(remote_sources(context), workspace, sink)
    except RuntimeError as exc:
        await Emitter(sink)('error', message=str(exc)[:2000], will_retry=False)
        raise


def program_requirement(command):
    """What a program card needs on a worker. A command whose executable can't be read from its
    text (a pipeline, a variable) is not checked in advance: it fails on the worker if it is missing."""
    from .capabilities import infer_command
    name = infer_command(command)
    return {'command': name} if name else {}


class ProgramRelay:
    """Shows a program in a card's transcript as one live tool row. `forward` is the sink of a job
    that runs it: the step events every worker sends become updates of that row."""
    def __init__(self, sink, tool_id='program', name='Program', summary=''):
        self.emit, self.sink = Emitter(sink), sink
        self.tool_id, self.name, self.summary = tool_id, name, summary[:300]
        self.tail, self.sent, self.started = [], 0.0, False

    def output(self):
        return '\n'.join(self.tail)[-(MAX_OUTPUT - 60):]

    async def tool(self, status, output=None):
        await self.emit('tool', id=self.tool_id, name=self.name, summary=self.summary, status=status, **({'output': output} if output else {}))

    async def start(self, **status):
        if self.started: return
        self.started = True
        if status: await self.emit('status', **status)
        await self.tool('running')

    async def line(self, text):
        self.tail.append(text)
        del self.tail[:-200]
        if time.monotonic() - self.sent >= .5:
            self.sent = time.monotonic()
            await self.tool('running', self.output())

    async def forward(self, event):
        kind = event.get('type')
        if kind == 'log': await self.line(event.get('text', ''))
        elif kind == 'step':
            if event.get('state') == 'running': await self.started_by_job()
        else: await self.sink(event)

    async def started_by_job(self):
        await self.start(state='working', agent='program')

    async def failed(self, error, announce=True):
        await self.tool('error', self.output() or str(error))
        if announce: await self.emit('error', message=str(error)[:2000], will_retry=False)

    async def finished(self):
        await self.tool('ok', self.output() or 'Finished with no output')


class CardRelay(ProgramRelay):
    """A program card: the status follows the program, so the card waits while it waits for a worker."""
    async def done(self, output, workspace=''):
        await self.finished()
        await self.emit('status', state='done')
        return {'state': 'done', 'agent': 'program', 'workspace': workspace, 'output': json.loads(json.dumps(output, default=str))}


class SourceRelay(ProgramRelay):
    """Fetching a run's sources: a row in its transcript, with no say over the run's status."""
    async def started_by_job(self):
        await self.start()


async def run_program(config, workspace, sink):
    """A program card on this machine (Trigger.dev cards run on the coordinator)."""
    from types import SimpleNamespace
    from .executor import _program
    relay = CardRelay(sink, summary=config['goal'])

    async def emit(event, /, **fields):
        await relay.forward({'type': event, **fields})

    step = SimpleNamespace(id='program', run=config['goal'], kind='program')
    try:
        output = await _program(step, {'input': {'text': config.get('programInput') or ''}, 'steps': {}}, workspace, emit)
    except Exception as exc:
        await relay.failed(exc)
        raise
    return await relay.done(output, str(workspace))


# Jobs a worker runs as ordinary program steps, so every worker can run them.
CARD_TIMEOUT_MINUTES = 1440  # the engines bound a card at 24 hours
# A step needs a declared executable when it can't be read from the command. A pipeline or a
# variable is not checked in advance on a card: it fails on the worker if it is missing.
NOTHING_TO_CHECK = 'echo'


def program_job(command, text=''):
    """A program card's step job. Its workspace stays on the worker, however large it gets."""
    from .capabilities import infer_command, shell_builtin
    from .definition import parse
    step = {'kind': 'program', 'run': command, 'timeout_minutes': CARD_TIMEOUT_MINUTES}
    if not infer_command(command) and not shell_builtin(command): step['requires_command'] = NOTHING_TO_CHECK
    flow = parse({'name': 'program-card', 'steps': {'program': step}})
    return {'flow': flow.to_json(), 'step': 'program', 'context': {'input': {'text': text}, 'steps': {}}, 'discardFiles': True}


def clone_job(repositories):
    """Clone the repositories in one step job; its files are the result. Values go through the
    template so they are quoted for the worker's shell. A commit can't be cloned shallowly."""
    from .definition import parse
    git, commands, values = 'git -c credential.interactive=false', [], {}
    for index, repo in enumerate(repositories):
        url, target, ref = f'{{{{ input.url{index} }}}}', f'{{{{ input.target{index} }}}}', f'{{{{ input.ref{index} }}}}'
        values.update({f'url{index}': repo['url'], f'target{index}': repo['target'], **({f'ref{index}': repo['ref']} if repo.get('ref') else {})})
        if repo.get('ref') and re.fullmatch(r'[0-9a-fA-F]{7,40}', repo['ref']):
            commands.append(f'{git} clone --no-hardlinks -- {url} {target} && {git} -C {target} checkout --detach {ref}')
        elif repo.get('ref'):
            commands.append(f'{git} clone --no-hardlinks --depth 1 --branch {ref} -- {url} {target}')
        else:
            commands.append(f'{git} clone --no-hardlinks --depth 1 -- {url} {target}')
    flow = parse({'name': 'attached-repositories', 'steps': {'clone': {'kind': 'program', 'run': ' && '.join(commands), 'requires_command': 'git'}}})
    return {'flow': flow.to_json(), 'step': 'clone', 'context': {'input': values, 'steps': {}}}
