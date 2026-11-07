"""Tiny read-only Confluence API for the real Markdown exporter's integration tests."""
from aiohttp import web
import base64
import time

PNG=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aJ1sAAAAASUVORK5CYII=')
TEXT=b'An unreferenced attachment.\n'


def confluence_app():
    app=web.Application()
    app['requests']=[]
    app['request_log']=[]
    app['broken_page']=None
    app['versions']={}
    app['attachment_versions']={}
    app['auth_schemes']=[]
    app['descendants']=['2','3']
    app['broken_attachment']=None
    app['expected_authorization']=None
    started=time.monotonic()
    @web.middleware
    async def trace(request,handler):
        entry={'path':request.path_qs,'started':round(time.monotonic()-started,3),'status':500}
        app['request_log'].append(entry)
        before=time.monotonic()
        try:
            response=await handler(request)
            entry['status']=response.status
            return response
        except web.HTTPException as exc:
            entry['status']=exc.status
            raise
        finally:
            entry['seconds']=round(time.monotonic()-before,3)
    app.middlewares.append(trace)
    def page(pid,base):
        body=f'<p><strong>content {pid}</strong></p>'
        if pid=='2':
            body+=('<table><tr><th>Name</th><th>Value</th></tr><tr><td>Answer</td><td>42</td></tr></table>'
                   '<div class="confluence-information-macro confluence-information-macro-information">'
                   '<p class="title">Important context</p><div class="confluence-information-macro-body">'
                   '<p>Macro body with <strong>formatting</strong>.</p></div></div>'
                   '<div class="expand-container"><div class="expand-control"><span class="expand-control-text">Details</span></div>'
                   '<div class="expand-content"><p>Expanded macro details.</p></div></div>'
                   '<pre><code class="language-python">print(42)\nexample = "[literal](/confluence/spaces/TEST/pages/2/Child)"</code></pre>'
                   '<p><code>[inline](/confluence/spaces/TEST/pages/2/Child)</code> — café שלום 🚀</p>'
                   '<img src="/download/attachments/2/icon.png">')
        if pid=='1':
            body+=('<p><a href="/pages/viewpage.action?pageId=2">Child page</a>'
                   '<a href="/confluence/spaces/TEST/pages/2/Child">Relative child</a>'
                   '<a href="/confluence/spaces/TEST/pages/2/Child#section">Child section</a>'
                   '<a href="/confluence/spaces/TEST/pages/4/Outside">Outside tree</a></p>'
                   '<img src="/download/attachments/1/diagram.png" alt="Diagram">')
        return {'id':pid,'title':f'Page {pid}','type':'page','status':'current',
                '_expandable':{'space':'/rest/api/space/TEST'},'version':{'number':app['versions'].get(pid,1),'when':'2026-01-01T00:00:00Z'},
                'ancestors':[] if pid=='1' else [{'id':'1','title':'Page 1','_expandable':{'space':'/rest/api/space/TEST'}}],
                'body':{kind:{'value':body} for kind in ('view','export_view','storage','editor2')},
                '_links':{'base':base,'webui':f'/pages/viewpage.action?pageId={pid}'}}
    async def api(request):
        app['requests'].append(request.path_qs)
        app['auth_schemes'].append(request.headers.get('Authorization','').split(' ',1)[0])
        if app['expected_authorization'] and request.headers.get('Authorization')!=app['expected_authorization']:
            raise web.HTTPUnauthorized()
        path=request.path; base=f'{request.scheme}://{request.host}'
        space={'key':'TEST','name':'Fixture','homepage':{'id':'1'},'description':{'plain':{'value':'Fixture'}}}
        if path=='/rest/api/space': return web.json_response({'results':[space]})
        if path=='/rest/api/space/TEST': return web.json_response(space)
        if path.startswith('/download/attachments/'):
            if path.rsplit('/',1)[-1]==app['broken_attachment']: raise web.HTTPForbidden()
            if path in ('/download/attachments/1/diagram.png','/download/attachments/2/icon.png'):
                return web.Response(body=PNG,content_type='image/png')
            if path=='/download/attachments/1/notes.txt': return web.Response(body=TEXT,content_type='text/plain')
        if path.endswith('/child/attachment'):
            pid=path.split('/')[4]
            entries={'1':[('att10','diagram.png',PNG,'image/png'),('att11','notes.txt',TEXT,'text/plain')],
                     '2':[('att20','icon.png',PNG,'image/png')]}.get(pid,[])
            attachments=[{'id':attid,'title':filename,'_expandable':{'space':'/rest/api/space/TEST'},
                'extensions':{'fileSize':len(data),'mediaType':media},'version':{'number':app['attachment_versions'].get(attid,1)},
                'container':{'id':pid,'title':f'Page {pid}'},'_links':{'download':f'/download/attachments/{pid}/{filename}'}}
                for attid,filename,data,media in entries]
            return web.json_response({'results':attachments,'size':len(attachments)})
        if path in ('/rest/api/content/search','/rest/api/content/1/descendant/page'):
            start=int(request.query.get('start','0'))
            pages=app['descendants']
            result={'results':[page(pages[start],base)] if start<len(pages) else [],'_links':{}}
            if start+1<len(pages): result['_links']['next']=path+f'?start={start+1}'
            return web.json_response(result)
        pid=path.rsplit('/',1)[-1]
        if pid==app['broken_page']: raise web.HTTPForbidden()
        if pid in ('1','2','3'): return web.json_response(page(pid,base))
        raise web.HTTPNotFound(text='Unexpected fixture request: '+request.path_qs)
    app.router.add_get('/{path:.*}',api)
    return app
