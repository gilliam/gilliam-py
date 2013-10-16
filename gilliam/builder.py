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

from .executor import ExecutorClient
from .util import thread


class BuilderClient(object):
    """API client for the builder service."""

    def __init__(self, client, executor=None):
        self.executor = executor or ExecutorClient(client)

    def build(self, repository, tag, infile, output,
              formation='builder', image='gilliam/base'):
        process = self.executor.run(formation, image, {}, ['/build/builder'])
        thread(process.attach, infile, output)
        result = process.wait()
        if result == 0:
            process.commit(repository, tag)
        return result
