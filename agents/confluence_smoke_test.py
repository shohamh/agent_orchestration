"""Import public Confluence documentation using the real environment setup path.

Outputs remain in ignored agents/runtime/confluence-public unless --output is supplied.
No credentials are used. Only small public documentation roots are selected.
"""
import argparse
import asyncio
import json
from datetime import datetime,timezone
from pathlib import Path

from flowkit.environment import prepare_sources,validate_sources
from flowkit.confluence_export import EXPORTER_VERSION,MANIFEST

SOURCES=[
    {'base_url':'https://cwiki.apache.org/confluence','page_id':'148639291','target':'apache-tika-tree',
     'url':'https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148639291'},
    {'base_url':'https://cwiki.apache.org/confluence','page_id':'109454103','target':'apache-tika',
     'url':'https://cwiki.apache.org/confluence/spaces/TIKA/pages/109454103/UsingGit'},
    {'base_url':'https://confluence.atlassian.com','page_id':'1056671473','target':'atlassian',
     'url':'https://confluence.atlassian.com/conf712/view-page-information-1056671473.html'},
    {'base_url':'https://shibboleth.atlassian.net/wiki','page_id':'2067399657','target':'shibboleth',
     'url':'https://shibboleth.atlassian.net/wiki/spaces/SP3/pages/2067399657/SystemRequirements'},
]


async def run_public_checks(output,only=None):
    output=Path(output).resolve();output.mkdir(parents=True,exist_ok=True)
    results=[]
    for source in SOURCES:
        if only and source['target'] not in only: continue
        item={k:v for k,v in source.items() if k!='url'}
        config={'confluence':[item]}
        print('Importing '+source['url'],flush=True)
        report=await prepare_sources(config,output)
        if not report[0]['ok']: raise RuntimeError(report[0]['error'])
        errors=await validate_sources(config,output)
        if errors: raise RuntimeError(errors)
        target=output/item['target'];manifest=json.loads((target/MANIFEST).read_text(encoding='utf-8'))
        pages=manifest['pages']
        for page in pages:
            path=target/page['file']
            assert path.suffix=='.md' and path.stat().st_size>100,path
        result={'url':source['url'],'target':str(target),'pages':len(pages),
                'markdown_bytes':sum((target/p['file']).stat().st_size for p in pages),
                'files':len(manifest['files']),'validated':True,'page_ids':[p['id'] for p in pages]}
        print(json.dumps(result),flush=True);results.append(result)
        (output/'verification.json').write_text(json.dumps({'exporter_version':EXPORTER_VERSION,
            'verified_at':datetime.now(timezone.utc).isoformat(),'sources':results},indent=2),encoding='utf-8')
    return results


async def main(args):
    return await run_public_checks(args.output,args.only)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',default=str(Path(__file__).parent/'runtime/confluence-public'))
    parser.add_argument('--only',nargs='+',choices=[s['target'] for s in SOURCES])
    asyncio.run(main(parser.parse_args()))
