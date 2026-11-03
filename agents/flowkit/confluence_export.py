"""Isolated adapter for the pinned Confluence Markdown Exporter.

Each invocation has its own exporter settings and credential environment. Only Markdown,
referenced attachments and a verifiable manifest enter the transferable workspace.
"""
import asyncio
import contextlib
import hashlib
import json
import os
import re
import sys
import tempfile
from importlib.metadata import version
from pathlib import Path
from urllib.parse import parse_qs,urljoin,urlsplit

from .artifacts import snapshot
from .restart import atomic_json

EXPORTER_VERSION = '5.6.0'
MANIFEST = 'confluence-export.json'


def source_identity(item):
    return {'base_url':item['base_url'].rstrip('/'), 'page_id':item['page_id'],
            'include_descendants':item.get('include_descendants',True),
            'attachments':item.get('attachments','referenced'), 'exporter_version':EXPORTER_VERSION}


def file_hashes(target):
    # Apply the normal workspace path/size rules to imports too.
    files=snapshot(target)
    import base64
    return {name:hashlib.sha256(base64.b64decode(entry['data'])).hexdigest()
            for name,entry in files.items() if name!=MANIFEST}


def local_manifest(item,target):
    try:
        data=json.loads((target/MANIFEST).read_text(encoding='utf-8'))
        if data['source']!=source_identity(item): raise ValueError('Confluence export belongs to another source or exporter version')
        if data['files']!=file_hashes(target): raise ValueError('Confluence Markdown or attachments are missing or have changed; re-import the source')
        if item['page_id'] not in {p['id'] for p in data['pages']} or not data['pages']:
            raise ValueError('Confluence root page is missing from the export')
        return data
    except (OSError,KeyError,json.JSONDecodeError):
        raise ValueError('Complete Confluence Markdown export manifest is unavailable; re-import the source') from None


def normalize_markdown_links(text,base_url,page_ids):
    """Fix root-relative Confluence links that the exporter leaves unresolved.

    Leave fenced and inline code alone. Other root-relative links become absolute, while
    exported page IDs resolve to their local Markdown files.
    """
    link=re.compile(r'(?<=\]\()(/(?!/)[^\s)]+)(?=\))')
    def replace(match):
        destination=match.group(1)
        parsed=urlsplit(destination)
        match_id=re.search(r'/spaces/[^/]+/pages/(\d+)(?:/|$)',parsed.path)
        page_id=match_id.group(1) if match_id else next(iter(parse_qs(parsed.query).get('pageId',[])),None)
        if page_id in page_ids:
            return page_id+'.md'+(('#'+parsed.fragment) if parsed.fragment else '')
        return urljoin(base_url+'/',destination)
    # Preserve block code first, then matching runs of inline-code backticks, including
    # multi-line spans and spans containing runs of a different length.
    protected=[]
    fence=None
    offset=0
    for line in text.splitlines(keepends=True):
        marker=re.match(r'^ {0,3}(`{3,}|~{3,})',line)
        if marker:
            token=marker.group(1)
            if fence is None: fence=token
            elif token[0]==fence[0] and len(token)>=len(fence): fence=None
            protected.append((offset,offset+len(line)))
        elif fence is not None or line.startswith(('    ','\t')):
            protected.append((offset,offset+len(line)))
        offset+=len(line)
    block_ranges=protected
    inline_ranges=[]
    block_index=0
    position=0
    while (start:=text.find('`',position))!=-1:
        while block_index<len(block_ranges) and block_ranges[block_index][1]<=start: block_index+=1
        if block_index<len(block_ranges) and block_ranges[block_index][0]<=start<block_ranges[block_index][1]:
            position=block_ranges[block_index][1];continue
        end=start
        while end<len(text) and text[end]=='`': end+=1
        closing=re.compile(r'(?<!`)'+re.escape(text[start:end])+r'(?!`)').search(text,end)
        if closing:
            finish=closing.end()
            inline_ranges.append((start,finish))
            position=finish
        else: position=end
    result=[]
    previous=0
    for start,end in sorted(block_ranges+inline_ranges):
        if end<=previous: continue
        if start>previous: result.append(link.sub(replace,text[previous:start]))
        result.append(text[max(previous,start):end])
        previous=end
    result.append(link.sub(replace,text[previous:]))
    return ''.join(result)


async def exporter(item,target,mode='export'):
    token=os.environ.get(item.get('token_env',''),'')
    user=os.environ.get(item.get('username_env',''),'')
    if item.get('token_env') and not token: raise ValueError(f'Missing worker credential: {item["token_env"]}')
    if item.get('username_env') and not user: raise ValueError(f'Missing worker credential: {item["username_env"]}')
    auth={'username':user,'api_token':token} if user else {'pat':token}
    # Keep exporter auth/config outside the workspace, including if a job is killed abruptly.
    with tempfile.TemporaryDirectory(prefix='confluence-settings-') as settings:
        env={k:v for k,v in os.environ.items() if not k.startswith('CME_')}
        env.update(CME_CONFIG_PATH=str(Path(settings)/'config.json'),CME_NON_INTERACTIVE='1',
                   CME_AUTH=json.dumps({'confluence':{item['base_url'].rstrip('/'):auth}}),
                   PYTHONUTF8='1',PYTHONUNBUFFERED='1')
        process=await asyncio.create_subprocess_exec(sys.executable,'-u','-m','flowkit.confluence_export',
            cwd=Path(__file__).resolve().parents[1],env=env,stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE,
            start_new_session=os.name!='nt' and not os.environ.get('FLOW_MANAGED_JOB'))
        from .executor import _own,_kill
        tree=_own(process.pid)
        try:
            payload=json.dumps({'source':source_identity(item),'target':str(target.resolve()),'mode':mode}).encode()
            try:
                stdout,stderr=await asyncio.wait_for(process.communicate(payload),timeout=600)
            except asyncio.TimeoutError:
                raise ValueError('Confluence Markdown Exporter exceeded its 10-minute timeout') from None
            if process.returncode:
                error=stderr.decode('utf-8','replace')[-4000:]
                for secret in (token,user):
                    if secret: error=error.replace(secret,'[redacted]')
                raise ValueError('Confluence Markdown Exporter failed: '+(error or f'exit {process.returncode}'))
            return json.loads(stdout)
        finally:
            _kill(process,tree)
            if tree: tree.close()


async def import_confluence(item,target):
    target=Path(target)
    target.parent.mkdir(parents=True,exist_ok=True)
    # An unsuccessful refresh cannot replace a previously complete import.
    with tempfile.TemporaryDirectory(prefix='.confluence-stage-',dir=target.parent) as directory:
        staged=Path(directory)/'export'
        staged.mkdir()
        pages=await exporter(item,staged)
        page_ids={p['id'] for p in pages}
        for page in pages:
            path=staged/page['file']
            text=path.read_text(encoding='utf-8')
            path.write_text(normalize_markdown_links(text,item['base_url'],page_ids),encoding='utf-8',newline='\n')
        atomic_json(staged/'index.json',pages)
        atomic_json(staged/MANIFEST,{'source':source_identity(item),'pages':pages,'files':file_hashes(staged)})
        local_manifest(item,staged)
        backup=Path(directory)/'previous'
        if target.exists(): target.rename(backup)
        try: staged.rename(target)
        except BaseException:
            if backup.exists(): backup.rename(target)
            raise


async def validate_confluence(item,target):
    data=local_manifest(item,Path(target))
    remote=await exporter(item,Path(target),mode='validate')
    saved={p['id']:(p['version'],p.get('attachments',{})) for p in data['pages']}
    if saved!={p['id']:(p['version'],p['attachments']) for p in remote}:
        raise ValueError('Confluence page tree, page versions or attachments changed; re-import the source')


def run(payload):
    if version('confluence-markdown-exporter')!=EXPORTER_VERSION:
        raise ValueError(f'Install confluence-markdown-exporter=={EXPORTER_VERSION} on this worker')
    source,target=payload['source'],Path(payload['target'])
    # Configure before importing modules which cache get_settings() at import time.
    os.environ['CME_EXPORT']=json.dumps({
        'output_path':str(target),'page_path':'{page_id}.md', 'page_href':'relative',
        'page_href_relative_only_if_ancestor_of':int(source['page_id']),
        'attachment_path':'attachments/{page_id}/{attachment_id}{attachment_extension}',
        'attachments_export':source['attachments'],'include_document_title':True,
        'page_breadcrumbs':False,'page_metadata_in_frontmatter':True,
        'confluence_url_in_frontmatter':'webui','enable_jira_enrichment':False,
        'comments_export':'none','skip_unchanged':False,'cleanup_stale':False,
        'save_log_to_file':False,'log_level':'ERROR','lockfile_name':'export.lock.json'})
    os.environ['CME_CONNECTION_CONFIG']=json.dumps({'max_workers':2,'timeout':30,'max_backoff_retries':2})
    from confluence_markdown_exporter.confluence import Page,export_pages
    from confluence_markdown_exporter.utils.lockfile import LockfileManager,ConfluenceLock
    from confluence_markdown_exporter.utils.rich_console import get_stats
    root=Page.from_id(int(source['page_id']),source['base_url'])
    if root.title=='Page not accessible' or root.id!=int(source['page_id']):
        raise ValueError('Confluence root page is not accessible')
    pages=[root,*root.descendants] if source['include_descendants'] else [root]
    if len(pages)>2000: raise ValueError('Confluence tree exceeds 2000 pages')
    expected={str(p.id) for p in pages}
    if payload['mode']=='export':
        LockfileManager.init()
        export_pages(pages)
        stats=get_stats()
        if stats.failed or stats.attachments_failed:
            raise ValueError(f'Incomplete export: {stats.failed} pages and {stats.attachments_failed} attachments failed')
        entries=ConfluenceLock.load(target/'export.lock.json').all_pages()
        if set(entries)!=expected: raise ValueError('Exporter did not publish every page in the requested tree')
        for entry in entries.values():
            path=target/entry.export_path
            if not path.resolve().is_relative_to(target.resolve()) or not path.is_file() or path.stat().st_size==0:
                raise ValueError('Exporter returned a missing or unsafe Markdown path')
    else: entries=None
    result=[]
    for page in pages:
        full=page if isinstance(page,Page) else Page.from_id(int(page.id),source['base_url'])
        if full.title=='Page not accessible' or (page.version and full.version and page.version.number!=full.version.number):
            raise ValueError(f'Confluence page {page.id} was inaccessible or changed during export')
        attachments={} if source['attachments']=='disabled' else {
            a.id:a.version.number if a.version else 0 for a in full._attachments_for_export()}
        if entries is not None:
            entry=entries[str(page.id)]
            if attachments!={key:value.version for key,value in entry.attachments.items()}:
                raise ValueError(f'Confluence attachments for page {page.id} were not completely exported')
            for attachment in entry.attachments.values():
                path=target/attachment.path
                if not path.resolve().is_relative_to(target.resolve()) or not path.is_file():
                    raise ValueError(f'Confluence attachment for page {page.id} is missing or unsafe')
        result.append({'id':str(page.id),'title':page.title,'version':page.version.number if page.version else 0,
                       'file':entries[str(page.id)].export_path if entries is not None else str(page.export_path),
                       'attachments':attachments})
    return result


if __name__=='__main__':
    try:
        request=json.load(sys.stdin)
        # Upstream progress/log output must not corrupt the result protocol.
        with contextlib.redirect_stdout(sys.stderr): result=run(request)
        print(json.dumps(result,ensure_ascii=False),flush=True)
    except BaseException as exc:
        # Upstream AuthNotConfiguredError inherits BaseException; never open an auth prompt.
        print(f'{type(exc).__name__}: {exc}',file=sys.stderr,flush=True)
        sys.exit(1)
