# Copyright 2013 Johan Rydberg.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from requests.auth import _basic_auth_str
from requests.adapters import BaseAdapter
from requests.compat import urlparse, unquote
from requests.models import Response
from requests.utils import get_auth_from_url

from .packages import websocket


class ResolveAdapter(object):
    """An adapter to request adapters that before sending the request,
    resolves the URL using a service registry client resolver.

    See :class:`Resolver <gilliam.service_registry.Resolver`.
    """

    def __init__(self, original, resolver):
        self.original = original
        self._resolver = resolver

    def send(self, request, *args, **kwargs):
        netloc = urlparse(request.url).netloc
        request.headers['Host'] = netloc
        request.prepare_url(self._resolver.resolve_url(request.url), {})
        return self.original.send(request, *args, **kwargs)

    def close(self):
        self.original.close()


class WebSocketAdapter(BaseAdapter):
    """Basic WebSocket adapter for `ws://` and `wss://` URLs.
    Supports proxies.
    """

    def proxy_headers(self, proxy):
        headers = {}
        username, password = get_auth_from_url(proxy)

        if username and password:
            username = unquote(username)
            password = unquote(password)
            headers['Proxy-Authorization'] = _basic_auth_str(username,
                                                             password)

        return headers

    def _proxy_from_url(self, url):
        u = urlparse(url)
        return (u.hostname, u.port)

    def _create_connection(self, url, proxies):
        proxies = proxies or {}
        proxy = (proxies.get(urlparse(url.lower()).scheme) 
                 or proxies.get('https') or proxies.get('http')) 

        if proxy:
            return websocket.create_connection(
                url,
                proxy=self._proxy_from_url(proxy),
                proxy_header=self.proxy_headers(proxy))
        else:
            return websocket.create_connection(url)

    def send(self, request, stream=False, timeout=None, verify=True,
             cert=None, proxies=None):
        conn = self._create_connection(request.url, proxies)
        return self.build_response(request, conn)

    def build_response(self, req, conn):
        """Builds a :class:`Response <requests.Response>` object from
        a websocket connection.

        :param req: The :class:`PreparedRequest <PreparedRequest>`
            used to generate the response.
        :param conn: The websocket connection.
        """
        response = Response()
        response.request = req
        response.raw = None
        response.websocket = conn
        response.status_code = conn.status
        # FIXME: what should we do with the proxy headers?
        response.headers.update(conn.resp_headers)

        if isinstance(req.url, bytes):
            response.url = req.url.decode('utf-8')
        else:
            response.url = req.url

        return response
