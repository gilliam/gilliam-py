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

from . import util, errors


class SchedulerClient(object):
    """Client for the scheduler API."""

    def __init__(self, client, host='api.scheduler.service', port=80):
        self.client = client
        self.base_url = 'http://%s:%d' % (host, port)

    def _url(self, fmt, *args):
        path_info = fmt % args
        return self.base_url + path_info

    def releases(self, formation):
        """Return an ."""
        try:
            return util.traverse_collection(
                self.client, self._url('/formation/%s/release', formation))
        except Exception, err:
            errors.convert_error(err)

    def instances(self, formation):
        try:
            return util.traverse_collection(
                self.client, self._url('/formation/%s/instances', formation))
        except Exception, err:
            errors.convert_error(err)

    def formations(self):
        try:
            return util.traverse_collection(self.client, self._url('/formation'))
        except Exception, err:
            errors.convert_error(err)

    def create_formation(self, formation):
        """Try to create a formation with the given name."""
        request = {'name': formation}
        try:
            response = self.client.post(self._url('/formation'),
                                        data=json.dumps(request))
            response.raise_for_status()
            return response.json()
        except Exception, err:
            errors.convert_error(err)

    def create_release(self, formation, name, author, message,
                       services):
        request = {'name': name, 'author': author, 'message': message,
                   'services': services}
        try:
            response = self.client.post(
                self._url('/formation/%s/release', formation),
                data=json.dumps(request))
            response.raise_for_status()
            return response.json()
        except Exception, err:
            errors.convert_error(err)

    def scale(self, formation, release, scales):
        request = {'scales': scales}
        try:
            response = self.client.post(self._url(
                    '/formation/%s/release/%s/scale', formation, release),
                data=json.dumps(request))
            response.raise_for_status()
            return response.json()
        except Exception, err:
            errors.convert_error(err)

    def migrate(self, formation, release, from_release=None):
        request = {'from': from_release}
        try:
            response = self.client.post(self._url(
                    '/formation/%s/release/%s/migrate', formation, release),
                data=json.dumps(request))
            response.raise_for_status()
            return response.json()
        except Exception, err:
            errors.convert_error(err)

    def spawn(self, formation, service, release, image, command,
              env, ports, assigned_to=None, requirements=[],
              rank=None):
        try:
            placement = {'requirements': requirements, 'rank': rank}
            request = {
                'service': service, 'release': release, 'image': image,
                'command': command, 'env': env, 'ports': ports,
                'assigned_to': assigned_to, 'placement': placement,
                }
            response = self.client.post(
                self._url('/formation/%s/instances', formation),
                data=json.dumps(request))
            response.raise_for_status()
            return response.json()
        except Exception, err:
            errors.convert_error(err)
