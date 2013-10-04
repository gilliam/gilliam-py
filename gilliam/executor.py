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

from .packages import websocket
from . import errors


def _thread(fn, *args, **kw):
    t = threading.Thread(target=fn, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t


class _RunningProcess(object):
    """A running process on an executor."""

    def __init__(self, client, location, process, clock=time):
        self.client = client
        self._process = process
        self._url = location
        self.status = None
        self._clock = clock

    def wait(self, timeout=None, interval=3):
        """Wait for the process to finish and return its exit code, or
        `None` if timeout.

        :params timeout: The number of seconds to wait for the process
            to exit.
        :params interval: How often to poll the executor for status
            changes.  Defaults to 3 seconds.

        :returns: The exit code of the process or `None` if the process
            didn't exit within the specified timeout.
        """
        def _wait(e):
            while self.status is None and not e.is_set():
                response = self.client.get(self._url)
                data = response.json()
                self.status = data['status']
                self._clock.sleep(interval)
                
        e = threading.Event()
        t = threading.Thread(target=_wait, args=(e,))
        t.daemon = True
        t.start()
        t.join(timeout)
        e.set()
        return self.status

    def attach(self, input, output):
        """Attach to the input and output streams of the process.

        Waits for the process to enter state `running` before
        attaching.

        .. note::

           This method call does **not** return, so call it in a
           thread.

        :params input: Read from this `file`-like object and pass it
            to stdin of the process.

        :param output: Write output from the process to this
            `file`-like object.
        """
        self._wait_for_state('running')

        url = '%s/attach' % (self._url,)
        adapter = self.client.get_adapter(url)
        if hasattr(adapter, '_resolver'):
            url = adapter._resolver.resolve_url(url)

        url = url.replace('http://', 'ws://')
        url = url.replace('https://', 'wss://')

        ws = websocket.create_connection(url)

        def send():
            print "START STUFF"
            for data in input:
                #print "I GOT SOME DATA TO SEND", data
                ws.send_binary(data)
            print "DONE"

        def recv():
            while True:
                data = ws.recv()
                if not data:
                    break
                else:
                    #print "WOW", data
                    output.write(data)
        
        sender = _thread(send)
        recver = _thread(recv)

        for t in (sender, recver):
            t.join()

    def commit(self, repository, tag, credentials=None):
        """Commit the container into an image.

        :param repository: Repository to store the image in.
        :type repository: str.

        :param tag: Tag to give the image.
        :type tag: str.

        :param credentials: Authentication credentials to pass to the registry.
        :type credentials: dict or `None`.

        :raises: HTTPError
        """
        request = {'repository': repository, 'tag': tag}
        if credentials:
            request['credentials'] = credentials
        response = self.client.post('%s/commit' % (self._url,),
                                    data=json.dumps(request))
        response.raise_for_status()

    def _wait_for_state(self, *states):
        """Wait for the process to enter one of the specified states.

        :params states: The expected state.
        :returns: The state.
        """
        while True:
            response = self.client.get(self._url)
            response.raise_for_status()
            data = response.json()
            if data['state'] in states:
                return data['state']
            else:
                self._clock.sleep(3)


class ExecutorClient(object):
    """Client interface for the executor."""

    def __init__(self, client, host='api.executor.service', port=9000):
        self.client = client
        self.base_url = 'http://%s:%d' % (host, port)

    def _url(self, fmt, *args):
        path_info = fmt % args
        return self.base_url + path_info

    def _run(self, formation, image, env, command, tty):
        request = {'formation': formation, 'image': image,
                   'env': env, 'command': command, 'tty': tty}
        try:
            response = self.client.post(self._url('/run'),
                                        data=json.dumps(request))
            response.raise_for_status()
        except Exception, err:
            errors.convert_error(err)
        else:
            return response.headers['location'], response.json()

    def run(self, formation, image, env, command, tty=False):
        location, response = self._run(
            formation, image, env, command, tty)
        return _RunningProcess(self.client, location, response)
