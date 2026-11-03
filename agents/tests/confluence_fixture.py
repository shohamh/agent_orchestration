"""Tiny read-only Confluence API for the real Markdown exporter's integration tests."""
from aiohttp import web
import base64

PNG=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aJ1sAAAAASUVORK5CYII=')


def confluence_app():
    app=web.Application()
    app['requests']=[]
    app['broken_page']=None
    app['versions']={}
    app['attachment_versions']={}
    app['auth_schemes']=[]
    def page(pid,base):
        body=f'<p><strong>content {pid}</strong></p>'
        if pid=='2':
            body+='<table><tr><th>Name</th><th>Value</th></tr><tr><td>Answer</td><td>42</td></tr></table><pre>print(42)</pre><img src="/download/attachments/2/icon.png">'
        if pid=='1': body+='<p><a href="/pages/viewpage.action?pageId=2">Child page</a><a href="/confluence/spaces/TEST/pages/2/Child">Relative child</a></p><img src="/download/attachments/1/diagram.png" alt="Diagram">'
        return {'id':pid,'title':f'Page {pid}','type':'page','status':'current',
                '_expandable':{'space':'/rest/api/space/TEST'},'version':{'number':app['versions'].get(pid,1),'when':'2026-01-01T00:00:00Z'},
                'ancestors':[] if pid=='1' else [{'id':'1','title':'Page 1','_expandable':{'space':'/rest/api/space/TEST'}}],
                'body':{kind:{'value':body} for kind in ('view','export_view','storage','editor2')},
                '_links':{'base':base,'webui':f'/pages/viewpage.action?pageId={pid}'}}
    async def api(request):
        app['requests'].append(request.path_qs)
        app['auth_schemes'].append(request.headers.get('Authorization','').split(' ',1)[0])
        path=request.path; base=f'{request.scheme}://{request.host}'
        space={'key':'TEST','name':'Fixture','homepage':{'id':'1'},'description':{'plain':{'value':'Fixture'}}}
        if path=='/rest/api/space': return web.json_response({'results':[space]})
        if path=='/rest/api/space/TEST': return web.json_response(space)
        if path in ('/download/attachments/1/diagram.png','/download/attachments/2/icon.png'): return web.Response(body=PNG,content_type='image/png')
        if path.endswith('/child/attachment'):
            pid=path.split('/')[4]
            attid,filename={'1':('att10','diagram.png'),'2':('att20','icon.png')}.get(pid,(None,None))
            attachments=[{'id':attid,'title':filename,'_expandable':{'space':'/rest/api/space/TEST'},
                'extensions':{'fileSize':len(PNG),'mediaType':'image/png'},'version':{'number':app['attachment_versions'].get(attid,1)},
                'container':{'id':pid,'title':f'Page {pid}'},'_links':{'download':f'/download/attachments/{pid}/{filename}'}}] if attid else []
            return web.json_response({'results':attachments,'size':len(attachments)})
        if path in ('/rest/api/content/search','/rest/api/content/1/descendant/page'):
            start=int(request.query.get('start','0'))
            result={'results':[page('2' if start==0 else '3',base)],'_links':{}}
            if start==0: result['_links']['next']=path+'?start=1'
            return web.json_response(result)
        pid=path.rsplit('/',1)[-1]
        if pid==app['broken_page']: raise web.HTTPForbidden()
        if pid in ('1','2','3'): return web.json_response(page(pid,base))
        raise web.HTTPNotFound(text='Unexpected fixture request: '+request.path_qs)
    app.router.add_get('/{path:.*}',api)
    return app
