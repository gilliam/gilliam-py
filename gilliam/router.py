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

import json
import threading
import time

from . import errors, util


class RouterClient(object):

    def __init__(self, client, host='api.router.service', port=80):
        self.client = client
        self.base_url = 'http://%s:%d' % (host, port)

    def _url(self, fmt, *args):
        path_info = fmt % args
        return self.base_url + path_info

    def routes(self):
        try:
            return util.traverse_collection(self.client, self._url('/route'))
        except Exception, err:
            errors.convert_error(err)

    def create(self, name, domain, path, target):
        request = {'name': name, 'domain': domain, 'path': path,
                   'target': target}
        try:
            response = self.client.post(self._url('/route'),
                                        data=json.dumps(request))
            response.raise_for_status()
            return response.json()
        except Exception, err:
            errors.convert_error(err)

    def delete(self, name):
        try:
            response = self.client.delete(self._url('/route/%s', name))
            response.raise_for_status()
        except Exception, err:
            errors.convert_error(err)
