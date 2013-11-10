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
import os
import time
import threading
from urlparse import urljoin, urlsplit, urlunsplit

from circuit import CircuitBreakerSet, CircuitOpenError
from requests.exceptions import (RequestException, ConnectionError,
                                 TooManyRedirects)
import requests

from . import errors


class _Registration(object):
    """A service registration."""

    def __init__(self, client, form_name, service, instance_name, data,
                 interval=3):
        self.log = logging.getLogger('{0}.reg.{1}/{2}.{3}'.format(
                __name__, form_name, service, instance_name))
        self.stopped = threading.Event()
        self.client = client
        self.form_name = form_name
        self.service = service
        self.instance_name = instance_name
        self.data = data
        self.interval = interval
        self._thread = None

    def _loop(self):
        uri = '/%s/%s.%s' % (self.form_name, self.service, self.instance_name)
        while not self.stopped.isSet():
            t0 = time.time()
            try:
                response = self.client._request(
                    'PUT', uri, data=json.dumps(self.data),
                    timeout=self.interval)
            except Exception:
                self.log.exception("could not talk to service registry")

            t1 = time.time()
            self.log.debug("time to update service registry: {0:.03f}".format(
                    t1 - t0))
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


class Resolver(object):
    """Resolver."""

    def __init__(self, client, search_domain=''):
        self.client = client
        self.search_domain = search_domain.split('.')

    def resolve_url(self, url):
        """Given a URL, return a resolved url."""
        u = urlsplit(url)
        host, port = self.resolve_host_port(u.hostname, int(u.port))
        return urlunsplit((u.scheme, '%s:%d' % (host, port),
                           u.path, u.query, u.fragment))

    def resolve_host_port(self, host, port):
        """Given a host and a port, return resolved host and port."""
        if '.' in host and not host.endswith(".service"):
            return host, port
        return self._resolve(host, port)

    def _resolve(self, host, port):
        parts = host.split('.')
        # trying to resolve a local name within the same formation.
        # if a search domain has not been specified, raise an error,
        # since we do not know how to proceed without one.
        if len(parts) == 1:
            # parts = [service]
            if not self.search_domain:
                raise errors.ResolveError("no search domain specified")
            host, port = self._resolve_any(port, parts[0], self.search_domain[0])
        elif len(parts) == 3:
            # parts = [service, formation, '.service']
            host, port = self._resolve_any(port, parts[0], parts[1])
        elif len(parts) == 4:
            # parts = [instance, service, formation, '.service']
            host, port = self._resolve_one(port, parts[0], parts[1],
                                           parts[2])
        return host, port

    def _select(self, formation, **filters):
        return [d for (k, d) in self.client.query_formation(formation)
                if all((d[attr].lower() == v) for (attr, v) in filters.items())]

    def _resolve_port(self, announcement, port):
        """Resolve port mapping in instance announcement."""
        if str(port) not in announcement['ports']:
            raise errors.ResolveError('port %d not exposed' % (port,))
        return int(announcement['ports'][str(port)])

    def _resolve_one(self, port, instance, service, formation):
        """Resolve a specific instance of a service in a formation.
        """
        alts = self._select(formation, service=service,
                            instance=instance.lower())
        if not alts:
            raise errors.ResolveError("%s.%s.%s.service:%d: no such instance" % (
                    instance, service, formation, port))

        # XXX: alts should only be one here, but we never know, right?
        alt = random.choice(alts)
        return alt['host'], self._resolve_port(alt, port)

    def _resolve_any(self, port, service, formation):
        """Resolve to any of the instances for the specified
        service.
        """
        alts = self._select(formation, service=service)
        if not alts:
            raise errors.ResolveError("%s.%s.service:%d: no instances" % (
                    service, formation, port))

        alt = random.choice(alts)
        return alt['host'], self._resolve_port(alt, port)


class ServiceRegistryClient(object):

    def __init__(self, clock, cluster_nodes=None):
        self.clock = clock
        self.cluster_nodes = []
        if cluster_nodes is None:
            cluster_nodes = os.getenv(
                'GILLIAM_SERVICE_REGISTRY', '').split(',')
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
        # backward compatible
        self.resolve = Resolver(self).resolve_url

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

    def register(self, form_name, service, instance_name, data):
        """Register an instance with a formation."""
        return _Registration(self, form_name, service, instance_name, data).start()

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


def make_client():
    """Construct a service registry client."""
    return ServiceRegistryClient(
        time, os.getenv('GILLIAM_SERVICE_REGISTRY_NODES', '').split(','))
