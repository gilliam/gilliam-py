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

from requests import exceptions as requests_exceptions

class GilliamError(Exception):
    """Base class for all errors."""


class ConnectionError(GilliamError):
    pass


class InternalServerError(GilliamError):
    pass


class ConflictError(GilliamError):
    pass


class ResolveError(GilliamError):
    pass


class PermissionError(GilliamError):
    pass


def convert_error(err):
    """Convenience function for converting from common catched errors
    to errors based on GilliamError.
    """
    if isinstance(err, requests_exceptions.ConnectionError):
        raise ConnectionError(str(err))
    elif isinstance(err, requests_exceptions.HTTPError):
        status_code = err.args[0]
        if status_code == 500:
            raise InternalServerError(str(err))
        elif status_code == 409:
            raise ConflictError(str(err))
        else:
            raise GilliamError(str(err))
    elif isinstance(err, requests_exceptions.RequestException):
        raise GilliamError(str(err))

    # we cannot convert the error, re-raise it.
    raise

