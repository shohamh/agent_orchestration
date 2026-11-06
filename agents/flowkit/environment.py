"""Materialize context on the setup worker, then validate it before dependencies are released."""
import asyncio
import json
import os
import re
import shutil
from pathlib import Path
from urllib.parse import urlsplit

from .artifacts import snapshot, restore, validate, MAX_BYTES
from .confluence_export import import_confluence, validate_confluence
from .restart import atomic_json
from .workers import selector


def configuration(value):
    if not isinstance(value, dict) or set(value)-{'agent', 'worker', 'repositories', 'files', 'confluence'}:
        raise ValueError('environment contains agent, worker, repositories, files and/or confluence')
    from .definition import AGENTS
    if value.get('agent', 'codex') not in AGENTS: raise ValueError('Invalid setup agent')
    selector(value.get('worker'))
    targets=[]
    for kind, allowed, required in (
        ('repositories', {'url','target','ref','shallow'}, {'url','target'}),
        ('files', {'source','target'}, {'source','target'}),
        ('confluence', {'base_url','page_id','target','token_env','username_env','include_descendants','attachments'}, {'base_url','page_id','target'})):
        items=value.get(kind, [])
        if not isinstance(items, list) or len(items)>50: raise ValueError(f'{kind} must be a list of at most 50 sources')
        for item in items:
            if not isinstance(item,dict) or set(item)-allowed or required-set(item) or any(
                    type(v) is not bool if k in ('include_descendants','shallow') else not isinstance(v,str) or not v or '\x00' in v
                    for k,v in item.items()):
                raise ValueError(f'Invalid {kind} source')
            target=item['target']
            validate({target:{'data':'', 'mode':420}})
            if any(p.startswith('.flow-') for p in target.split('/')): raise ValueError('Context target uses a reserved runtime directory')
            lowered=target.casefold()
            if any(lowered==old or lowered.startswith(old+'/') or old.startswith(lowered+'/') for old in targets):
                raise ValueError('Context targets must not overlap')
            targets.append(lowered)
            url=item.get('url', item.get('base_url',''))
            parsed=urlsplit(url)
            if parsed.scheme in ('http','https') and (parsed.username or parsed.password): raise ValueError('Use worker credential helpers or token_env, not credentials in URLs')
            if kind=='repositories' and (url.startswith('-') or item.get('ref','').startswith('-')): raise ValueError('Invalid git URL/ref')
            if kind=='confluence':
                if item.get('attachments','referenced') not in ('referenced','all','disabled'): raise ValueError('Invalid Confluence attachments option')
                if parsed.scheme not in ('http','https') or not parsed.hostname or parsed.query or parsed.fragment: raise ValueError('Invalid Confluence base_url')
                if not re.fullmatch(r'[0-9]+', item['page_id']): raise ValueError('Confluence page_id must be numeric')
                for key in ('token_env','username_env'):
                    if key in item and not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',item[key]): raise ValueError('Invalid credential environment variable name')
    return value


async def git(*args, cwd=None):
    process=await asyncio.create_subprocess_exec('git', *args, cwd=cwd, env={**os.environ,'GIT_TERMINAL_PROMPT':'0'},
        stdin=asyncio.subprocess.DEVNULL,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,
        start_new_session=os.name!='nt' and not os.environ.get('FLOW_MANAGED_JOB'))
    from .executor import _own, _kill
    tree=_own(process.pid)
    try:
        stdout,stderr=await asyncio.wait_for(process.communicate(), timeout=300)
        if process.returncode: raise ValueError(stderr.decode('utf-8','replace')[-2000:] or 'git failed')
        return stdout.decode('utf-8','replace').strip()
    finally:
        _kill(process,tree)
        if tree: tree.close()


async def clone(item,target):
    """A shallow clone keeps a repository within a run's transfer limit. It only works for a branch
    or tag (or the default branch), so a commit id falls back to a full clone."""
    if item.get('shallow'):
        branch=('--branch',item['ref']) if item.get('ref') else ()
        try:
            await git('clone','--no-hardlinks','--depth','1',*branch,'--',item['url'],str(target))
            return
        except ValueError:
            if not item.get('ref'): raise
            shutil.rmtree(target,ignore_errors=True)
    await git('clone','--no-hardlinks','--',item['url'],str(target))
    if item.get('ref'): await git('-C',str(target),'checkout','--detach',await resolved_ref(item['ref'],target))


async def resolved_ref(ref,target):
    """A branch other than the default exists only as origin/<branch> after a full clone."""
    try:
        await git('-C',str(target),'rev-parse','--verify','--quiet',ref+'^{commit}')
        return ref
    except ValueError:
        return 'origin/'+ref


def target_path(workspace,item):
    path=workspace/item['target']
    if not path.resolve().is_relative_to(workspace.resolve()) or path.is_symlink(): raise ValueError('Unsafe context destination')
    return path


async def prepare_sources(config, workspace):
    configuration(config)
    report=[]
    for kind in ('repositories','files','confluence'):
        for item in config.get(kind,[]):
            try:
                target=target_path(workspace,item)
                target.parent.mkdir(parents=True,exist_ok=True)
                if kind=='repositories':
                    if not target.exists():
                        await clone(item,target)
                    await validate_repository(item,target)
                elif kind=='files':
                    source=Path(item['source']).expanduser()
                    if source.is_symlink(): raise ValueError('Source file may not be a symlink')
                    if source.is_dir(): restore(await asyncio.to_thread(snapshot,source),target)
                    else:
                        if source.stat().st_size>MAX_BYTES: raise ValueError('Context file exceeds transfer limit')
                        await asyncio.to_thread(shutil.copy2,source,target)
                else:
                    await import_confluence(item,target)
                report.append({'source':item['target'],'kind':kind,'ok':True})
            except Exception as exc:
                # Do not echo response headers or credentials from APIs.
                report.append({'source':item['target'],'kind':kind,'ok':False,'error':str(exc)[:2500]})
    atomic_json(workspace/'.flow-setup-report.json',report)
    return report


async def validate_repository(item,target):
    if await git('-C',str(target),'rev-parse','--is-inside-work-tree')!='true': raise ValueError('Missing git worktree')
    actual=await git('-C',str(target),'remote','get-url','origin')
    if actual!=item['url']: raise ValueError('Repository origin differs from the requested source')
    head=await git('-C',str(target),'rev-parse','HEAD')
    if await git('-C',str(target),'ls-files','--deleted'):
        raise ValueError('Repository has missing tracked files; restore the worktree before completing setup')
    if item.get('ref') and head != await git('-C',str(target),'rev-parse',await resolved_ref(item['ref'],target)+'^{commit}'): raise ValueError('Repository revision differs from requested ref')


async def validate_sources(config,workspace):
    errors=[]
    for kind in ('repositories','files','confluence'):
        for item in config.get(kind,[]):
            try:
                target=target_path(workspace,item)
                if kind=='repositories': await validate_repository(item,target)
                elif kind=='files':
                    source=Path(item['source']).expanduser()
                    if source.is_dir():
                        if await asyncio.to_thread(snapshot,source) != await asyncio.to_thread(snapshot,target): raise ValueError('Copied directory is incomplete or differs from source')
                    elif not target.is_file() or source.read_bytes()!=target.read_bytes(): raise ValueError('Copied file is missing or differs from source')
                else:
                    await validate_confluence(item,target)
            except Exception as exc: errors.append(f'{item["target"]}: {str(exc)[:2000]}')
    return errors


async def setup_environment(config,workspace,receive,sink):
    from agentdriver.events import Emitter
    from agentdriver.session import run_session
    emit=Emitter(sink)
    report=await prepare_sources(config,workspace)
    for item in report:
        await emit('tool',id='setup:'+item['source'],name='Prepare '+item['kind'],summary=item['source'],
                   status='ok' if item['ok'] else 'error',output=item.get('error','Context copied'))
    instructions=('Prepare this environment and repair any source errors. Review .flow-setup-report.json. '
                  'Do not change the requested source locations. Credentials are available only through worker environment variables. '
                  'Explain any remaining login or network issues to the human. Wait for review.\n'+json.dumps(config,ensure_ascii=False)+'\n'+json.dumps(report,ensure_ascii=False))
    while True:
        await run_session({'goal':instructions,'agent':config.get('agent','codex'),'workspace':str(workspace),'autoDone':False,
                           'quietDemo':config.get('agent')=='demo'},receive,sink)
        errors=await validate_sources(config,workspace)
        if not errors: return {'sources':report,'validated':True}
        instructions='Setup is incomplete. Repair these sources before marking done:\n'+'\n'.join(errors)
        await emit('error',message=instructions,will_retry=False)
        await emit('status',state='awaiting_input',reason='Setup validation failed; dependencies remain blocked')
