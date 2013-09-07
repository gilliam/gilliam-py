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

"""Functionality for service discovery."""

import logging
import json
import random
import requests
from requests.exceptions import (RequestException, ConnectionError,
                                 TooManyRedirects)
from urlparse import urljoin

import threading

from gevent.event import Event
import gevent

from circuit import CircuitBreakerSet, CircuitOpenError


class _Registration(object):
    """A service registration."""

    def __init__(self, client, form_name, instance_name, data,
                 interval=3):
        self.stopped = threading.Event()
        self.client = client
        self.form_name = form_name
        self.instance_name = instance_name
        self.data = data
        self.interval = interval
        self._thread = None

    def _loop(self):
        uri = '/%s/%s' % (self.form_name, self.instance_name)
        while not self.stopped.isSet():
            try:
                response = self.client._request(
                    'PUT', uri, data=json.dumps(self.data))
            except Exception:
                # FIXME: log exception
                pass
            self.stopped.wait(self.interval)

    def start(self):
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True
        self._thread.start()
        return self

    def stop(self, timeout=None):
        self.stopped.set()
        self._thread.join(timeout)


class _FormationCache(object):
    """A cache of instance data for a formation."""

    def __init__(self, client, form_name, factory, interval):
        self.client = client
        self.form_name = form_name
        self.factory = factory
        self.interval = interval
        self._thread = None
        self._cache = {}
        self._stopped = threading.Event()
        self._running = threading.Event()
        self._lock = threading.Lock()

    def start(self):
        self._thread = threading.Thread(target=self._loop)
        self._thread.daemon = True
        self._thread.start()
        self._running.wait(timeout=0.1)
        return self

    def stop(self, timeout=None):
        self._stopped.set()
        self._thread.join(timeout)

    def _update(self):
        with self._lock:
            self._cache = dict(self.client.query_formation(
                    self.form_name, self.factory))

    def _loop(self):
        while not self._stopped.isSet():
            self._update()
            self._running.set()
            self._stopped.wait(self.interval)

    def query(self):
        """Return all instances and their names."""
        with self._lock:
            return dict(self._cache)


class ServiceRegistryClient(object):

    def __init__(self, clock, cluster_nodes):
        self.clock = clock
        self.cluster_nodes = []
        for cluster_node in cluster_nodes:
            if not cluster_node.startswith('http://'):
                cluster_node = 'http://%s' % (cluster_node,)
            self.cluster_nodes.append((cluster_node, requests.Session()))
        random.shuffle(self.cluster_nodes)
        self.breaker = CircuitBreakerSet(clock.time, logging.getLogger(
                'service-discovery-client'))
        self.breaker.handle_error(RequestException)
        self.breaker.handle_error(ConnectionError)
        self.breaker.handle_error(TooManyRedirects)

    def _request(self, method, uri, **kwargs):
        """Issue a request to SOME of the nodes in the cluster."""
        for node, session in self.cluster_nodes:
            try:
                with self.breaker.context(node):
                    response = session.request(method, urljoin(node, uri),
                                               **kwargs)
                    if response.status_code >= 500:
                        raise RequestException()
                    return response
            except CircuitOpenError:
                continue
        else:
            raise Exception("NO MACHIEN TO TALK TOOO")

    def register(self, form_name, instance_name, data):
        """Register an instance with a formation."""
        return _Registration(self, form_name, instance_name, data).start()

    def build_announcement(self, formation, service, instance,
                           ports={}, **kwargs):
        announcement = {
            'formation': formation, 'service': service,
            'instance': instance, 'ports': ports.copy(),
            }
        announcement.update(kwargs)
        return announcement

    def query_formation(self, form_name, factory=dict):
        """Query all instances of a formation.
        
        Will return a generator that yields (instance name, data) for
        each instance.

        @param factory: Data factory.  Will be passed a JSON object of
            the instance data, expects to return a representation of
            that data.
        """
        response = self._request('GET', '/%s' % (form_name,))
        response.raise_for_status()
        for key, data in response.json().items():
            yield (key, factory(data))

    def formation_cache(self, form_name, factory=dict, interval=15):
        """Return a cache for a specific formation that will be kept
        up to date until stopped.
        """
        return _FormationCache(self, form_name, factory, interval).start()

    # FIXME: refactor

    def _resolve_port(self, d, port):
        if str(port) not in d['ports']:
            raise ValueError("instance do not expose port")
        return d['ports'][str(port)]

    def _resolve_any(self, port, formation, service):
        alts = [d for (k, d) in self.query_formation(formation)
                if k.startswith(service + '.')]
        if not alts:
            raise Exception("no instances")
        alt = random.choice(alts)
        return alt['host'], self._resolve_port(alt, port)

    def _resolve_specific(self, port, formation, service, name):
        alts = [d for (k, d) in self.query_formation(formation)
                if k.startswith(service + '.' + name)]
        if not alts:
            raise Exception("no instances")
        alt = random.choice(alts)
        return alt['host'], self._resolve_port(alt, port)

    def resolve(self, url):
        """Resolve a URL into a direct url."""
        u = urlparse.urlsplit(url)
        assert u.hostname.endswith('.service'), "must end with .service"
        parts = u.hostname.split('.')
        if len(parts) == 4:
            hostname, port = self._resolve_specific(u.port, parts[2],
                                                    parts[1], parts[0])
        elif len(parts) == 3:
            hostname, port = self._resolve_any(u.port, parts[1], parts[0])
        netloc = '%s:%d' % (hostname, port)
        return urlparse.urlunsplit((u.scheme, netloc, u.path,
                                    u.query, u.fragment))
