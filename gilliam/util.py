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

from urlparse import urljoin
import threading


def traverse_collection(httpclient, url):
    """Traverse a collection, yielding every item."""
    while True:
        response = httpclient.get(url)
        response.raise_for_status()
        collection = response.json()
        for item in collection['items']:
            yield item
        if not 'next' in collection['links']:
            break
        url = urljoin(url, collection['links']['next'])


def thread(fn, *args, **kw):
    t = threading.Thread(target=fn, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t
