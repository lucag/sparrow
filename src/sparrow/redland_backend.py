import os

from io import StringIO
from zope.interface import implementer

try:
    import RDF
except ImportError:
    RDF = None

from sparrow.base_backend import BaseBackend
from sparrow.error import ConnectionError, TripleStoreError, QueryError
from sparrow.interfaces import ITripleStore, ISPARQLEndpoint
from sparrow.utils import (parse_sparql_result,
                           ntriples_to_dict,
                           ntriples_to_json)

@implementer(ITripleStore, ISPARQLEndpoint)
class RedlandTripleStore(BaseBackend):

    def __init__(self):
        self._nsmap = {}

    def connect(self, dburi):
        self._model = model_from_uri(dburi, contexts='yes')

    def disconnect(self):
        del self._model

    def contexts(self):
        return [c.literal_value['string'].decode('utf8') for c in
                self._model.get_contexts()]

    def register_prefix(self, prefix, namespace):
        self._nsmap[prefix] = namespace

    def add_rdfxml(self, data, context_name, base_uri):
        data = self._get_file(data)
        self._add_stream(self._parse(data, 'rdfxml', base_uri), context_name)
        
    def add_ntriples(self, data, context_name):
        data = self._get_file(data)
        self._add_stream(self._parse(data, 'ntriples', '-'), context_name)

    def add_turtle(self, data, context_name):
        data = self._get_file(data)
        self._add_stream(self._parse(data, 'turtle', '-'), context_name)

    def _add_stream(self, stream, context_name):
        # if isinstance(context_name, str):
        #     context_name = context_name.encode('utf8')
        self._model.add_statements(stream, RDF.Node(context_name))
        
    def _parse(self, file, format, base_uri=None):            
        if format == 'turtle':
            parser = RDF.TurtleParser()
        else:
            parser = RDF.Parser(format)

        # if isinstance(base_uri, unicode):
        #     base_uri = base_uri.encode('utf8')
        data = file.read()
        file.close()
        try:
            stream = parser.parse_string_as_stream(data, base_uri)
        except RDF.RedlandError as err:
            raise TripleStoreError(err)
        if stream is None:
            raise TripleStoreError('Parsing RDF Failed')
        return stream

    def get_rdfxml(self, context_name, pretty=False):
        if pretty:
            format = 'rdfxml-abbrev'
        else:
            format = 'rdfxml'
        return self._serialize(context_name, format)

    def get_ntriples(self, context_name):
        return self._serialize(context_name, 'ntriples')
        
    def get_turtle(self, context_name):
        return self._serialize(context_name, 'turtle')
    
    def _serialize(self, context_name, format):
        # this sucks, we need a temp model because contexts can not
        # be serialized :(

        # if isinstance(context_name, unicode):
        #     context_name = context_name.encode('utf8')
            
        stream = self._model.as_stream(RDF.Node(context_name))            
        return self._serialize_stream(stream, format)
        
    def _serialize_stream(self, stream, format):
        temp = model_from_uri('memory')
        temp.add_statements(stream)
        serializer = RDF.Serializer(name=format)
        for prefix, ns in self._nsmap.items():
            serializer.set_namespace(prefix, ns)

        return StringIO(serializer.serialize_model_to_string(temp))

    def remove_rdfxml(self, data, context_name, base_uri):
        data = self._get_file(data)
        self._remove(data, 'rdfxml', context_name, base_uri)

    def remove_turtle(self, data, context_name):
        data = self._get_file(data)
        self._remove(data, 'turtle', context_name, '-')

    def remove_ntriples(self, data, context_name):
        data = self._get_file(data)
        self._remove(data, 'ntriples', context_name, '-')

    def _remove(self, file, format, context, base_uri):
        # if isinstance(context, unicode):
        #     context = context.encode('utf8')
        context = RDF.Node(context)
        
        stream = self._parse(file, format, base_uri)
        for statement in stream:
            self._model.remove_statement(statement, context)
        
    def clear(self, context):
        # if isinstance(context, unicode):
        #     context = context.encode('utf8')
        self._model.remove_statements_with_context(RDF.Node(context))
    
    def count(self, context=None):
        result = None
        if context is None:
            result = len(self._model)
        return result

    def _query(self, sparql):
        # if isinstance(sparql, unicode):
        #     sparql = sparql.encode('utf8')
        query = RDF.SPARQLQuery(sparql)
        
        try:
            result = query.execute(self._model)
        except RDF.RedlandError as err:
            raise QueryError(err)
        return result

    def select(self, sparql):
        result = self._query(sparql)
        if not result.is_bindings():
            raise QueryError('SELECT Query did not return bindings')
        return parse_sparql_result(result.to_string())
        
    def ask(self, sparql):
        result = self._query(sparql)
        if not result.is_boolean():
            raise QueryError('ASK Query did not return a boolean')
        
        return result.get_boolean()

    def construct(self, sparql, format):
        out_format = format
        if format in ['json', 'dict']:
            out_format = 'ntriples'
        result = self._query(sparql)
        if not result.is_graph():
            raise QueryError('CONSTRUCT Query did not return a graph')
        
        m = RDF.Model()
        stream = result.as_stream()
        if stream is None:
            return
        result = self._serialize_stream(stream, out_format)
        if format == 'json':
            result = ntriples_to_json(result)
        elif format == 'dict':
            result = ntriples_to_dict(result)
        return result

    
def model_from_uri(uri=None, **opts):
    if RDF is None:
        raise ConnectionError(
            'Redland support is not available, install from librdf.org')
    
    options = []
    for key, val in opts.items():
        options.append("%s='%s'" % (key, val))
        
    options = ','.join(options)
    if uri == None or uri == 'memory':
        options += ",hash-type='memory'"
        store = RDF.Storage(storage_name='hashes',
                            name='memory_store',
                            options_string=options)
    elif uri.startswith('sqlite://'):
        file = uri[9:]
        try:
            store = RDF.Storage(storage_name='sqlite',
                                name=file,
                                options_string=options)
        except RDF.RedlandError:
            raise ConnectionError("Can't connect to %s" % uri)
    elif uri.startswith('bdb://'):
        file = uri[6:]
        if not os.path.isfile(file+'-sp2o.db') and not 'new=' in options:
            options += ",new='yes'"
        options += ",hash-type='bdb'"
        store = RDF.Storage(storage_name='hashes',
                            name=file,
                            options_string=options)
    elif uri.startswith('mysql://'):
        uri = uri[8:]
        userpass, hostdb = uri.split('@')
        user, pwd = userpass.split(':')
        host, db = hostdb.split('/')
        if ':' in host:
            host, port = host.split(':')
        else:
            port = '3306'
        if '#' in db:
            db, name = db.split('#')
        else:
            name = 'main'

        options += (",host='%s',port='%s',database='%s',user='%s',"
                    "password='%s'" % (host, port, db, user, pwd))
        try:
            store = RDF.Storage(storage_name='mysql',
                                name=name,
                                options_string=options)
        except RDF.RedlandError:
            raise ConnectionError("Can't connect to %s" % uri)
    else:
        raise ConnectionError('Unknown dburi: %s' % uri)
        
    return RDF.Model(store)

    
