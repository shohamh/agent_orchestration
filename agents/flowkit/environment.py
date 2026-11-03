"""Materialize context on the setup worker, then validate it before dependencies are released."""
import asyncio
import hashlib
import html
import json
import os
import re
import shutil
from pathlib import Path
from urllib.parse import urljoin, urlsplit, quote

import aiohttp
from .artifacts import snapshot, restore, validate, MAX_BYTES
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
        ('repositories', {'url','target','ref'}, {'url','target'}),
        ('files', {'source','target'}, {'source','target'}),
        ('confluence', {'base_url','page_id','target','token_env','username_env'}, {'base_url','page_id','target'})):
        items=value.get(kind, [])
        if not isinstance(items, list) or len(items)>50: raise ValueError(f'{kind} must be a list of at most 50 sources')
        for item in items:
            if not isinstance(item,dict) or set(item)-allowed or required-set(item) or any(not isinstance(v,str) or not v or '\x00' in v for v in item.values()):
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


def target_path(workspace,item):
    path=workspace/item['target']
    if not path.resolve().is_relative_to(workspace.resolve()) or path.is_symlink(): raise ValueError('Unsafe context destination')
    return path


async def wiki_pages(item):
    base=item['base_url'].rstrip('/')
    token=os.environ.get(item.get('token_env',''),'')
    user=os.environ.get(item.get('username_env',''),'')
    if item.get('token_env') and not token: raise ValueError(f'Missing worker credential: {item["token_env"]}')
    if item.get('username_env') and not user: raise ValueError(f'Missing worker credential: {item["username_env"]}')
    auth=aiohttp.BasicAuth(user,token) if user else None
    headers={'Authorization':f'Bearer {token}'} if token and not user else {}
    async with aiohttp.ClientSession(auth=auth,headers=headers,timeout=aiohttp.ClientTimeout(total=30)) as client:
        async def get(url):
            if urlsplit(url).netloc != urlsplit(base).netloc or urlsplit(url).scheme != urlsplit(base).scheme: raise ValueError('Confluence pagination left its configured origin')
            async with client.get(url, allow_redirects=False) as response:
                if response.status!=200: raise ValueError(f'Confluence returned HTTP {response.status}')
                body=bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    body.extend(chunk)
                    if len(body)>8*1024*1024: raise ValueError('Confluence API response exceeds 8 MiB')
                return json.loads(body)
        path=f'{base}/rest/api/content/{item["page_id"]}'
        root=await get(path+'?expand=body.storage,version')
        if str(root.get('id'))!=item['page_id']: raise ValueError('Confluence returned the wrong root page')
        pages={str(root['id']):root}
        url=path+'/descendant/page?expand=body.storage,version&limit=25&start=0'
        visited=set()
        while url:
            if url in visited: raise ValueError('Confluence pagination did not advance')
            visited.add(url)
            result=await get(url)
            for page in result['results']:
                pid=str(page['id'])
                if not re.fullmatch(r'[0-9]+',pid): raise ValueError('Invalid Confluence page id')
                if 'storage' not in page.get('body',{}): page=await get(f'{base}/rest/api/content/{pid}?expand=body.storage,version')
                pages[pid]=page
                if len(pages)>2000: raise ValueError('Confluence tree exceeds 2000 pages')
            next_link=(result.get('_links') or {}).get('next')
            url=urljoin(base+'/', next_link) if next_link else None
        return pages


def wiki_content(page):
    body=page['body']['storage']['value']
    return ('<!doctype html><meta charset="utf-8"><title>'+html.escape(page.get('title',''))+'</title>\n'+body).encode('utf-8')


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
                        await git('clone','--no-hardlinks','--',item['url'],str(target))
                        if item.get('ref'): await git('-C',str(target),'checkout','--detach',item['ref'])
                    await validate_repository(item,target)
                elif kind=='files':
                    source=Path(item['source']).expanduser()
                    if source.is_symlink(): raise ValueError('Source file may not be a symlink')
                    if source.is_dir(): restore(await asyncio.to_thread(snapshot,source),target)
                    else:
                        if source.stat().st_size>MAX_BYTES: raise ValueError('Context file exceeds transfer limit')
                        await asyncio.to_thread(shutil.copy2,source,target)
                else:
                    pages=await wiki_pages(item)
                    target.mkdir(parents=True,exist_ok=True)
                    total=0
                    for pid,page in pages.items():
                        content=wiki_content(page); total+=len(content)
                        if total>MAX_BYTES: raise ValueError('Confluence export exceeds transfer limit')
                        (target/f'{pid}.html').write_bytes(content)
                    atomic_json(target/'index.json',[{'id':pid,'title':page.get('title',''),'file':f'{pid}.html'} for pid,page in pages.items()])
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
    if item.get('ref') and head != await git('-C',str(target),'rev-parse',item['ref']+'^{commit}'): raise ValueError('Repository revision differs from requested ref')


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
                    pages=await wiki_pages(item)
                    index=json.loads((target/'index.json').read_text(encoding='utf-8'))
                    if {p['id'] for p in index}!=set(pages): raise ValueError('Confluence index is incomplete')
                    for pid,page in pages.items():
                        if (target/f'{pid}.html').read_bytes()!=wiki_content(page): raise ValueError(f'Confluence page {pid} is missing or incomplete')
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
