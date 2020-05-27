import codecs
from abc import ABC
from io import BytesIO, StringIO

from six.moves import urllib_request as urllib2

from sparrow.error import TripleStoreError
from sparrow.utils import (json_to_ntriples,
                           dict_to_ntriples,
                           ntriples_to_json,
                           ntriples_to_dict)


class BaseBackend(ABC):

    def _is_uri(self, data):
        if not isinstance(data, str):
            return False
        return data.startswith('http://') or data.startswith('file://')

    def _get_file(self, data):
        if self._is_uri(data):
            if data.startswith('file://'):
                return open(data[7:], 'rb')
            elif data.startswith('http://'):
                return urllib2.urlopen(data)
        elif all(hasattr(data, a) for a in ('read', 'seek', 'close')):
            return data
        else:
            return BytesIO(bytes(data, encoding='utf-8'))

    def add_json(self, data, context_name):
        data = self._get_file(data)
        try:
            data = json_to_ntriples(data)
        except ValueError as err:
            raise TripleStoreError(err)

        self.add_ntriples(data, context_name)

    def add_dict(self, data, context_name):
        data = dict_to_ntriples(data)
        self.add_ntriples(data, context_name)

    def get_json(self, context_name):
        data = self.get_ntriples(context_name)
        return ntriples_to_json(data)

    def get_dict(self, context_name):
        data = self.get_ntriples(context_name)
        return ntriples_to_dict(data)

    def remove_json(self, data, context_name):
        data = self._get_file(data)
        data = json_to_ntriples(data)
        self.remove_ntriples(data, context_name)

    def remove_dict(self, data, context_name):
        data = dict_to_ntriples(data)
        self.remove_ntriples(data, context_name)

    def add_ntriples(self, data, context_name):
        pass

    def remove_ntriples(self, data, context_name):
        pass

    def get_ntriples(self, context_name):
        pass
