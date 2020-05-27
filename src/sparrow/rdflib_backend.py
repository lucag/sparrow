from __future__ import print_function

import traceback
from typing import Optional

from io import BytesIO
from rdflib import URIRef, plugin
from rdflib.store import Store
from six.moves import StringIO
from zope.interface import implementer

# try:
import rdflib
from rdflib.graph import Graph, ConjunctiveGraph
from rdflib.plugins.memory import IOMemory
# except ImportError as e:
#     print('problems importing rdflib: %s', e)
#     rdflib = Graph = ConjunctiveGraph = IOMemory = None

from .base_backend import BaseBackend
from .error import ConnectionError, TripleStoreError, QueryError
from .interfaces import ITripleStore, ISPARQLEndpoint
from .utils import parse_sparql_result, ntriples_to_dict, ntriples_to_json


@implementer(ITripleStore, ISPARQLEndpoint)
class RDFLibTripleStore(BaseBackend):
    _store = None  # type: Optional[IOMemory]

    def __init__(self):
        self._nsmap = {}
        self._store = None

    def connect(self, dburi):
        if rdflib is None:
            raise ConnectionError('RDFLib backend is not installed')
        if dburi == 'memory':
            self._store = plugin.get('IOMemory', Store)()
            self._store.graph_aware = False # fixes context bug in Python 3
        else:
            raise ConnectionError('Unknown database config: %s' % dburi)

    def disconnect(self):
        pass

    @staticmethod
    def _rdflib_format(format):
        return {'ntriples': 'nt',
                'rdfxml': 'xml',
                'turtle': 'n3'}[format]

    def contexts(self):
        return [str(c.identifier) for c in self._store.contexts()]

    def _get_context(self, context_name):
        # type: (str) -> Optional[Graph]
        context = URIRef(context_name) if context_name else context_name
        for ctxt in self._store.contexts():
            if ctxt.identifier == context:
                return ctxt

        # print('_get_context: returning None for ctx %s' % context_name)
        # traceback.print_stack(limit=3)
        return None

    def register_prefix(self, prefix, namespace):
        self._nsmap[prefix] = namespace

    def _parse(self, graph, file, format, base_uri=None):
        try:
            graph.parse(file, base_uri, format)
        except Exception as err:
            # each parser throws different errors,
            # there's an ntriples error, but the rdfxml
            # parser throws no errors so you end up with
            # a saxparser exception.
            # The n3 parser just silently fails
            # without any tra
            # print(f'>>> exception:\n{traceback.format_exc()}')
            raise TripleStoreError(err)

    def add_rdfxml(self, data, context, base_uri):
        data = self._get_file(data)
        graph = Graph(store=self._store, identifier=context)
        self._parse(graph, data, 'xml', base_uri)

    def add_ntriples(self, data, context):
        data = self._get_file(data)
        graph = Graph(store=self._store, identifier=context)
        self._parse(graph, data, 'nt')

    def add_turtle(self, data, context):
        data = self._get_file(data)
        graph = Graph(store=self._store, identifier=context)
        self._parse(graph, data, 'n3')

    def _serialize(self, graph, format, pretty=False):
        for prefix, namespace in self._nsmap.items():
            graph.bind(prefix, namespace)
        return StringIO(graph.serialize(format=format).decode('utf-8'))
        # return BytesIO(graph.serialize(format=format))

    def get_rdfxml(self, context, pretty=False):
        return self._serialize(self._get_context(context), 'xml')

    def get_turtle(self, context):
        return self._serialize(self._get_context(context), 'n3')

    def get_ntriples(self, context):
        return self._serialize(self._get_context(context), 'nt')

    def remove_rdfxml(self, data, context, base_uri):
        data = self._get_file(data)
        self._remove(data, context, 'xml', base_uri)

    def remove_ntriples(self, data, context):
        data = self._get_file(data)
        self._remove(data, context, 'nt')

    def remove_turtle(self, data, context):
        data = self._get_file(data)
        self._remove(data, context, 'n3')

    def _remove(self, file, context, format, base_uri=None):
        graph = Graph()
        self._parse(graph, file, format=format, base_uri=base_uri)
        context = self._get_context(context)
        assert context is not None
        for triple in graph:
            self._store.remove(triple, context)

    def clear(self, context):
        # type: (str) -> None
        context = self._get_context(context)
        self._store.remove((None, None, None), context)

    def count(self, context=None):
        context = self._get_context(context)

        if context is not None:
            return len(context)

        return len(self._store)

    def _query(self, sparql):
        try:
            result = ConjunctiveGraph(self._store).query(sparql)
        except Exception as err:
            raise QueryError(err)
        return result

    def select(self, sparql):
        result = self._query(sparql)
        return parse_sparql_result(result.serialize())

    def ask(self, sparql):
        result = self._query(sparql)
        return result.askAnswer

    def construct(self, sparql, format):
        out_format = format
        if format in ('json', 'dict'):
            out_format = 'ntriples'

        result = self._query(sparql)
        if not result:
            raise QueryError('CONSTRUCT Query did not return a graph')

        serialized = self._serialize(result, self._rdflib_format(out_format))
        # result = self._serialize(result, self._rdflib_format(out_format))
        if format == 'json':
            return ntriples_to_json(BytesIO(serialized.getvalue().encode('utf-8')))
        elif format == 'dict':
            return ntriples_to_dict(BytesIO(serialized.getvalue().encode('utf-8')))
        else:
            return serialized
