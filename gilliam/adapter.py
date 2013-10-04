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

from requests.adapters import HTTPAdapter as RequestsHTTPAdapter


class ResolvingHTTPAdapter(RequestsHTTPAdapter):
    """A version of :class:`requests.adapters.HTTPAdapter` that
    resolves URLs using the service registry.
    """

    def __init__(self, resolver, *args, **kw):
        super(ResolvingHTTPAdapter, self).__init__(*args, **kw)
        self._resolver = resolver

    def send(self, request, *args, **kwargs):
        # XXX: this modifies the prepared request.
        request.prepare_url(self._resolver.resolve_url(request.url), {})
        return super(ResolvingHTTPAdapter, self).send(request, *args, **kwargs)
