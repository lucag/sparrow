from cStringIO import StringIO
import urllib2

from sparrow.utils import (json_to_ntriples,
                           dict_to_ntriples,
                           ntriples_to_json,
                           ntriples_to_dict)
from sparrow.error import TripleStoreError

class BaseBackend(object):


    def _is_uri(self, data):
        if not isinstance(data, basestring):
            return False
        return data.startswith('http://') or data.startswith('file://')

    def _get_file(self, data):
        if self._is_uri(data):
            if data.startswith('file://'):
                return open(data[7:], 'r')
            elif data.startswith('http://'):
                return urllib2.urlopen(data)
        elif (hasattr(data, 'read') and
              hasattr(data, 'seek') and
              hasattr(data, 'close')):
            return data
        
        return StringIO(data)

    def add_json(self, data, context_name):
        data = self._get_file(data)
        try:
            data = json_to_ntriples(data)
        except ValueError, err:
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
