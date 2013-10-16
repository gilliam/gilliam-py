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
from .util import thread
from . import errors


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
        t.join(timeout or 2**31)
        e.set()
        return self.status

    def attach(self, input, output, replay=False):
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

        :param replay: If `True`, replay output data that has been
            captured earlier.
        """
        self.wait_for_state('running')

        params = {}
        if replay:
            params['logs'] = 1

        url = '%s/attach' % (self._url,)
        url = url.replace('http://', 'ws://').replace('https://', 'wss://')

        resp = self.client.get(url, params=params)
        resp.raise_for_status()
        ws = resp.websocket

        def send():
            try:
                for data in input:
                    ws.send_binary(data)
            except websocket.WebSocketConnectionClosedException:
                pass

        def recv():
            try:
                while True:
                    data = ws.recv()
                    if not data:
                        break
                    else:
                        output.write(data)
            except websocket.WebSocketConnectionClosedException:
                pass
        
        sender = thread(send)
        recver = thread(recv)

        for t in (sender, recver):
            t.join()

    def commit(self, repository, tag):
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
        response = self.client.post('%s/commit' % (self._url,),
                                    data=json.dumps(request))
        response.raise_for_status()

    def wait_for_state(self, *states):
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

    def resize_tty(self, width, height):
        """Set size of process TTY to `width` x `height`."""
        params = {'w': str(width), 'h': str(height)}
        response = self.client.post('%s/resize' % (self._url,),
                                    params=params)
        response.raise_for_status()


class ExecutorClient(object):
    """Client interface for the executor."""

    def __init__(self, client, host='api.executor.service', port=9000):
        self.client = client
        self.base_url = 'http://%s:%d' % (host, port)

    def _url(self, fmt, *args):
        path_info = fmt % args
        return self.base_url + path_info

    def _convert_image_error(self, detail):
        if detail['code'] == 403:
            return errors.PermissionError(detail['message'])
        else:
            return errors.InternalServerError(detail['message'])

    def push_image(self, image, auth={}):
        """Push an image to its registry.

        Which registry is determined based on `image`; if it contains
        a dot or a colon, it is considered a hostname, otherwise it is
        determinted to be a username on the official registry.

        Will yield status messages while the image is being pushed.

        .. code-block: python

           >>> for status in executor.push_image('name/image'):
           ...     print status
           ...

        :param auth: Credentials to authenticate with against the
            registry.

        :raises: PermissionError, InternalServerError, GilliamError

        :returns: An iterator that yields status messages while the
            image is pushed.
        """
        request = {'image': image, 'auth': auth}
        try:
            response = self.client.post(self._url('/_push_image'),
                                        data=json.dumps(request),
                                        stream=True)
            response.raise_for_status()
        except Exception, err:
            errors.convert_error(err)

        ITER_CHUNK_SIZE = 1
        for text in response.iter_lines(ITER_CHUNK_SIZE):
            status = json.loads(text)
            if 'error' in status:
                raise self._convert_image_error(
                    status['errorDetail'])
            yield status

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
