import os
from io import BytesIO
from unittest import TestCase

from sparrow.error import TripleStoreError, QueryError
from sparrow.interfaces import ITripleStore, ISPARQLEndpoint
from sparrow.tests.utils import to_tuple

TESTFILE = 'wine'
FORMATS = ['ntriples', 'rdfxml', 'turtle', 'json']


def open_test_file(fmt):
    extension = {'rdfxml': '.rdf',
                 'ntriples': '.nt',
                 'turtle': '.ttl',
                 'json': '.json'}
    filename = 'wine' + extension[fmt]
    return open(os.path.join(os.path.dirname(__file__), filename), 'rb')


class TripleStoreTest(TestCase):

    def test_broken_parsing(self: ITripleStore):
        self.assertRaises(
            TripleStoreError,
            self.db.add_rdfxml,
            '@', 'test', 'http://example.org')
        self.assertRaises(
            TripleStoreError,
            self.db.add_ntriples,
            '@', 'test')
        self.assertRaises(
            TripleStoreError,
            self.db.add_turtle,
            '@', 'test')
        self.assertRaises(
            TripleStoreError,
            self.db.add_json,
            '@', 'test')

    def test_rdfxml_parsing(self: ITripleStore):
        with open_test_file('rdfxml') as f:
            self.db.add_rdfxml(f, 'test', 'file://wine.rdf')
            triples = self.db.count('test') or self.db.count()
            if not triples is None:
                self.assertTrue(triples > 1500)

    def test_ntriples_parsing(self: ITripleStore):
        with open_test_file('ntriples') as f:
            self.db.add_ntriples(f, 'test')
            triples = self.db.count('test') or self.db.count()
            if not triples is None:
                self.assertTrue(triples > 1500)

    def test_turtle_parsing(self: ITripleStore):
        with open_test_file('turtle') as f:
                self.db.add_turtle(f, 'test')
                triples = self.db.count('test') or self.db.count()
                if not triples is None:
                    self.assertTrue(triples > 1500)

    def test_json_parsing(self: ITripleStore):
        with open_test_file('json') as f:
            self.db.add_json(f, 'test')
            triples = self.db.count('test') or self.db.count()
            if not triples is None:
                self.assertTrue(triples > 1500)

    def test_base_uri_parsing(self: ITripleStore):
        # data = StringIO('''<?xml version="1.0"?>
        #                    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
        #                        <rdf:Description rdf:about="">
        #                           <foo:name xmlns:foo="http://foobar.com#">bar</foo:name>
        #                        </rdf:Description>
        #                    </rdf:RDF>
        #     ''')
        data = BytesIO(
            b'''<?xml version="1.0"?>
                <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
                    <rdf:Description rdf:about="">
                        <foo:name xmlns:foo="http://foobar.com#">bar</foo:name>
                    </rdf:Description>
                </rdf:RDF>
            ''')
        self.db.add_rdfxml(data, 'test', 'http://example.org/')
        result = self.db.get_ntriples('test').read()
        self.assertEqual(result.strip()[:-1].strip(),
                         '<http://example.org/> <http://foobar.com#name> "bar"')

    def test_remove_statements(self: ITripleStore):
        with open_test_file('ntriples') as f:
            self.db.add_ntriples(f, 'test')
            fp = BytesIO(
                b'<http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine> '
                b'<http://www.w3.org/2000/01/rdf-schema#label> '
                b'"Wine Ontology" .\n')

            self.db.remove_ntriples(fp, 'test')
            data = self.db.get_ntriples('test').read()
            self.assertTrue('Wine Ontology' not in data)

    def test_ntriples_serializing(self: ITripleStore):
        with open_test_file('ntriples') as f:
            self.db.add_ntriples(f, 'test')
            data = self.db.get_ntriples('test').read()
            self.assertTrue('Wine Ontology' in data)

    def test_rdfxml_serializing(self: ITripleStore):
        with open_test_file('rdfxml') as f:
            self.db.add_rdfxml(f, 'test', 'file://wine.rdf')
            self.db.register_prefix(
                'vin',
                'http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#')
            data = self.db.get_rdfxml('test').read()
            self.assertTrue('Wine Ontology' in data)
            self.assertTrue('xmlns:vin' in data)

    def test_turtle_serializing(self: ITripleStore):
        with open_test_file('turtle') as f:
            self.db.add_turtle(f, 'test')
            self.db.register_prefix(
                'vin',
                'http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#')
            data = self.db.get_turtle('test').read()
            self.assertTrue('Wine Ontology' in data)
            self.assertTrue('@prefix vin' in data)

    def test_clear(self: ITripleStore):
        count = self.db.count('test')
        if count is None:
            return
        self.assertEqual(count, 0)
        self.db.add_ntriples(open_test_file('ntriples'), 'test')
        count = self.db.count()
        self.assertTrue(count > 1500)
        self.db.clear('test')
        count = self.db.count()
        self.assertEqual(count, 0)

    def test_contexts(self: ITripleStore):
        self.assertEqual(list(self.db.contexts()), [])
        self.db.add_ntriples(open_test_file('ntriples'), 'a')
        self.db.add_ntriples(open_test_file('ntriples'), 'b')
        self.db.add_ntriples(open_test_file('ntriples'), 'c')
        self.assertEqual(sorted(list(self.db.contexts())), ['a', 'b', 'c'])
        self.db.clear('b')
        self.assertEqual(sorted(list(self.db.contexts())), ['a', 'c'])
        self.db.clear('a')
        self.db.clear('c')
        self.assertEqual(list(self.db.contexts()), [])


class TripleStoreQueryTest(TestCase):
    def test_ask(self: ITripleStore):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
        ASK { ?x rdfs:label "Wine Ontology"}
        """
        self.assertTrue(self.db.ask(q))
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
        ASK { ?x rdfs:label "FooBar"}
        """
        self.assertFalse(self.db.ask(q))

    def test_select(self: ITripleStore):
        q = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix vin: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#>
        select ?grape
        where { ?grape a vin:WineGrape .}
        """
        result = self.db.select(q)
        self.assertEqual(len(result), 16)
        grapes = sorted(
            [r['grape']['value'].split('#')[-1] for r in result])

        self.assertEqual(grapes, [
            'CabernetFrancGrape', 'CabernetSauvignonGrape',
            'ChardonnayGrape', 'CheninBlancGrape', 'GamayGrape',
            'MalbecGrape', 'MerlotGrape', 'PetiteSyrahGrape',
            'PetiteVerdotGrape', 'PinotBlancGrape', 'PinotNoirGrape',
            'RieslingGrape', 'SangioveseGrape', 'SauvignonBlancGrape',
            'SemillonGrape', 'ZinfandelGrape'])

    def test_select_literal_language(self: ISPARQLEndpoint):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX vin: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#>
        SELECT ?label
        WHERE { vin:Wine rdfs:label ?label .}
        """
        result = sorted(to_tuple(r) for r in self.db.select(q))
        self.assertEqual(result,
                         [to_tuple(d) for d in ({'label': {'type': 'literal',
                                                           'value': 'wine',
                                                           'lang': 'en'}},
                                                {'label': {'type': 'literal',
                                                           'value': 'vin',
                                                           'lang': 'fr'}})])

    def test_select_literal_datatype(self: ISPARQLEndpoint):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX vin: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#>
        SELECT ?year
        WHERE { vin:Year1998 vin:yearValue ?year .}
        """
        result = self.db.select(q)
        self.assertEqual(
            sorted(result),
            [{u'year': {
                'type': u'literal',
                'value': u'1998',
                'datatype': u'http://www.w3.org/2001/XMLSchema#positiveInteger'}}])

    def test_construct(self: ISPARQLEndpoint):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
        CONSTRUCT {?x rdfs:label "Wine Ontology"}
        WHERE { ?x rdfs:label "Wine Ontology" .}
        """
        fp = self.db.construct(q, 'ntriples')
        self.assertEqual(
            fp.read().strip()[:-1].strip(),
            ('<http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine> '
             '<http://www.w3.org/2000/01/rdf-schema#label> "Wine Ontology"'))

        fp.close()

    def test_construct_dict(self: ISPARQLEndpoint):
        self.maxDiff = None
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
        CONSTRUCT {?x rdfs:label "Wine Ontology"}
        WHERE { ?x rdfs:label "Wine Ontology" .}
        """
        data = self.db.construct(q, 'dict')
        self.assertDictEqual(data, {
            'http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine': {
                'http://www.w3.org/2000/01/rdf-schema#label': [
                    {'type': 'literal', 'value': 'Wine Ontology'}
                ]
            }
        })
        
    def test_broken_query(self):
        self.assertRaises(QueryError,
                          self.db.select,
                          'foo')
        self.assertRaises(QueryError,
                          self.db.ask,
                          'foo')
        self.assertRaises(QueryError,
                          self.db.construct,
                          'foo', 'rdfxml')
