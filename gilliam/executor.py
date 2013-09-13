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

from . import errors


class _RunningProcess(object):
    """A running process on an executor."""

    def __init__(self, client, location, process):
        self.client = client
        self._process = process
        self._url = location
        self.status = None

    def wait(self, timeout=None):
        """Wait for the process to finish and return its exit code, or
        C{None} if timeout.
        """
        def _wait(e):
            while self.status is None and not e.is_set():
                response = self.client.get(self._url)
                data = response.json()
                self.status = data['status']
                time.sleep(3)
                
        e = threading.Event()
        t = threading.Thread(target=_wait, args=(e,))
        t.daemon = True
        t.start()
        t.join(timeout)
        e.set()
        return self.status

    def attach(self, input, output):
        """Attach to process input and output streams."""
        self._wait_for_state('running')
        response = self.client.post('%s/attach' % (self._url,),
                                    data=input, stream=True)
        for data in response.iter_content():
            output.write(data)

    def commit(self, repository, tag):
        """Commit the running process."""
        request = {'repository': repository, 'tag': tag}
        response = self.client.post('%s/commit' % (self._url,),
                                    data=json.dumps(request))
        response.raise_for_status()

    def _wait_for_state(self, state):
        while True:
            response = self.client.get(self._url)
            response.raise_for_status()
            data = response.json()
            if data['state'] == state:
                return state
            else:
                time.sleep(3)


class ExecutorClient(object):
    """Client interface for the executor."""

    def __init__(self, client, host='api.executor.service', port=9000):
        self.client = client
        self.base_url = 'http://%s:%d' % (host, port)

    def _url(self, fmt, *args):
        path_info = fmt % args
        return self.base_url + path_info

    def _run(self, formation, image, env, command):
        request = {'formation': formation, 'image': image,
                   'env': env, 'command': command}
        try:
            response = self.client.post(self._url('/run'),
                                        data=json.dumps(request))
            response.raise_for_status()
        except Exception, err:
            errors.convert_error(err)
        else:
            return response.headers['location'], response.json()

    def run(self, formation, image, env, command):
        location, response = self._run(formation, image, env, command)
        return _RunningProcess(self.client, location, response)
