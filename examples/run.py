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

"""Simple example of how to run an arbitraty image on an executor."""

import contextlib
from functools import partial
import os
import sys
import termios
import threading

from requests.adapters import HTTPAdapter
import requests

from gilliam.adapter import (WebSocketAdapter, ResolveAdapter)
from gilliam.service_registry import make_client, Resolver
from gilliam import ExecutorClient


def _thread(fn, *args, **kw):
    t = threading.Thread(target=fn, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t


@contextlib.contextmanager
def console():
    fd = sys.stdin.fileno()
    isatty = os.isatty(fd)

    if isatty:
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
        oldterm = termios.tcgetattr(fd)
        newattr = oldterm[:]
        newattr[3] = newattr[3] & ~termios.ICANON & ~termios.ECHO
        termios.tcsetattr(fd, termios.TCSANOW, newattr)

    try:
        yield 
    finally:
        if isatty:
            termios.tcsetattr(fd, termios.TCSAFLUSH, oldterm)


def example():
    http = requests.Session()
    resolver = Resolver(make_client())
    http.mount('http://', ResolveAdapter(HTTPAdapter(), resolver))
    http.mount('ws://', ResolveAdapter(WebSocketAdapter(), resolver))

    client = ExecutorClient(http)
    reader = iter(partial(sys.stdin.read, 1), '')

    process = client.run('examples', 'ubuntu', {}, ['/bin/bash'],
                         tty=os.isatty(sys.stdin.fileno()))
    with console():
        _thread(process.attach, reader, sys.stdout, replay=True)
        exit_code = process.wait()

    sys.exit(exit_code)


if __name__ == '__main__':
    example()

        
