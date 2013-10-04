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

import json
import sys
import threading
import tty
from functools import partial

from gilliam.adapter import ResolvingHTTPAdapter
from gilliam.service_registry import make_client, Resolver
from gilliam import ExecutorClient

import contextlib
import os

import requests
import curses
import termios


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
    http.mount('http://', ResolvingHTTPAdapter(Resolver(make_client())))

    client = ExecutorClient(http)
    reader = iter(partial(sys.stdin.read, 1), '')

    with console():
        process = client.run('examples', 'ubuntu', {}, ['/bin/bash'],
                             tty=os.isatty(sys.stdin.fileno()))
        t = _thread(process.attach, reader, sys.stdout)
        exit_code = process.wait()

    sys.exit(exit_code)


if __name__ == '__main__':
    example()

        
