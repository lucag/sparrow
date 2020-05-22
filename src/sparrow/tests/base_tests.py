import os
import StringIO
from unittest import TestCase, TestSuite, makeSuite, main

from sparrow.interfaces import ITripleStore, ISPARQLEndpoint
from sparrow.error import TripleStoreError, QueryError

TESTFILE = 'wine'
FORMATS = ['ntriples', 'rdfxml', 'turtle', 'json']

def open_test_file(format):
    extension = {'rdfxml': '.rdf',
                 'ntriples': '.nt',
                 'turtle': '.ttl',
                 'json': '.json'}
    filename = 'wine' + extension[format]
    return open(
        os.path.join(os.path.dirname(__file__), filename), 'r')

class TripleStoreTest(TestCase):

    def test_broken_parsing(self):
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

    def test_rdfxml_parsing(self):
        self.db.add_rdfxml(open_test_file('rdfxml'), 'test', 'file://wine.rdf')
        triples = self.db.count('test') or self.db.count()
        if not triples is None:
            self.assertTrue(triples > 1500)

    def test_ntriples_parsing(self):
        self.db.add_ntriples(open_test_file('ntriples'), 'test')
        triples = self.db.count('test') or self.db.count()
        if not triples is None:
            self.assertTrue(triples > 1500)

    def test_turtle_parsing(self):
        self.db.add_turtle(open_test_file('turtle'), 'test')
        triples = self.db.count('test') or self.db.count()
        if not triples is None:
            self.assertTrue(triples > 1500)

    def test_json_parsing(self):
        self.db.add_json(open_test_file('json'), 'test')
        triples = self.db.count('test') or self.db.count()
        if not triples is None:
            self.assertTrue(triples > 1500)

    def test_base_uri_parsing(self):
        data = StringIO.StringIO('''<?xml version="1.0"?>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">
 <rdf:Description rdf:about="">
  <foo:name xmlns:foo="http://foobar.com#">bar</foo:name>
 </rdf:Description>
</rdf:RDF>''')
        self.db.add_rdfxml(data, 'test', 'http://example.org')
        result = self.db.get_ntriples('test').read()
        self.assertEquals(result.strip()[:-1].strip(),
                          '<http://example.org> <http://foobar.com#name> "bar"')
        
    def test_remove_statements(self):
        self.db.add_ntriples(open_test_file('ntriples'), 'test')
        fp = StringIO.StringIO(
            '<http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine> '
            '<http://www.w3.org/2000/01/rdf-schema#label> '
            '"Wine Ontology" .\n')
        
        self.db.remove_ntriples(fp, 'test')
        data = self.db.get_ntriples('test').read()
        self.assertTrue('Wine Ontology' not in data)

    def test_ntriples_serializing(self):
        self.db.add_ntriples(open_test_file('ntriples'), 'test')
        data = self.db.get_ntriples('test').read()
        self.assertTrue('Wine Ontology' in data)

    def test_rdfxml_serializing(self):
        self.db.add_rdfxml(open_test_file('rdfxml'), 'test', 'file://wine.rdf')
        self.db.register_prefix(
            'vin',
            'http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#')
        data = self.db.get_rdfxml('test').read()
        self.assertTrue('Wine Ontology' in data)
        self.assertTrue('xmlns:vin' in data)

    def test_turtle_serializing(self):
        self.db.add_turtle(open_test_file('turtle'), 'test')
        self.db.register_prefix(
            'vin',
            'http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#')
        data = self.db.get_turtle('test').read()
        self.assertTrue('Wine Ontology' in data)
        self.assertTrue('@prefix vin' in data)
        
    def test_clear(self):
        count = self.db.count('test')
        if count is None:
            return
        self.assertEquals(count , 0)
        self.db.add_ntriples(open_test_file('ntriples'), 'test')
        count = self.db.count()
        self.assertTrue(count > 1500)
        self.db.clear('test')
        count = self.db.count()
        self.assertEquals(count , 0)

    def test_contexts(self):
        self.assertEquals(list(self.db.contexts()), [])
        self.db.add_ntriples(open_test_file('ntriples'), 'a')
        self.db.add_ntriples(open_test_file('ntriples'), 'b')
        self.db.add_ntriples(open_test_file('ntriples'), 'c')
        self.assertEquals(sorted(list(self.db.contexts())), [u'a', u'b', u'c'])
        self.db.clear('b')
        self.assertEquals(sorted(list(self.db.contexts())), [u'a', u'c'])
        self.db.clear('a')
        self.db.clear('c')
        self.assertEquals(list(self.db.contexts()), [])
        
    
class TripleStoreQueryTest(TestCase):
    def test_ask(self):
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

    def test_select(self):
        q = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix vin: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#>
        select ?grape
        where { ?grape a vin:WineGrape .}
        """
        result = self.db.select(q)
        self.assertEquals(len(result), 16)
        grapes = sorted(
            [r['grape']['value'].split('#')[-1] for r in result])
        
        self.assertEquals(grapes, [
            u'CabernetFrancGrape', u'CabernetSauvignonGrape',
            u'ChardonnayGrape', u'CheninBlancGrape', u'GamayGrape',
            u'MalbecGrape', u'MerlotGrape', u'PetiteSyrahGrape',
            u'PetiteVerdotGrape', u'PinotBlancGrape', u'PinotNoirGrape',
            u'RieslingGrape', u'SangioveseGrape', u'SauvignonBlancGrape',
            u'SemillonGrape', u'ZinfandelGrape'])

    def test_select_literal_language(self):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX vin: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#>
        SELECT ?label
        WHERE { vin:Wine rdfs:label ?label .}
        """
        result = self.db.select(q)
        self.assertEquals(sorted(result),
                           [{u'label': {'type': u'literal',
                                        'value': u'wine',
                                        'lang': u'en'}},
                            {u'label': {'type': u'literal',
                                        'value': u'vin',
                                        'lang': u'fr'}}])

    def test_select_literal_datatype(self):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX vin: <http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine#>
        SELECT ?year
        WHERE { vin:Year1998 vin:yearValue ?year .}
        """
        result = self.db.select(q)
        self.assertEquals(
            sorted(result),
            [{u'year': {
            'type': u'literal',
            'value': u'1998',
            'datatype': u'http://www.w3.org/2001/XMLSchema#positiveInteger'}}])
        
    def test_construct(self):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
        CONSTRUCT {?x rdfs:label "Wine Ontology"}
        WHERE { ?x rdfs:label "Wine Ontology" .}
        """
        fp = self.db.construct(q, 'ntriples')
        self.assertEquals(
            fp.read().strip()[:-1].strip(),
            ('<http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine> '
             '<http://www.w3.org/2000/01/rdf-schema#label> "Wine Ontology"'))


        fp.close()
        
    def test_construct_dict(self):
        q = """
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>        
        CONSTRUCT {?x rdfs:label "Wine Ontology"}
        WHERE { ?x rdfs:label "Wine Ontology" .}
        """
        data = self.db.construct(q, 'dict')
        self.assertEquals(data, {
            u'http://www.w3.org/TR/2003/PR-owl-guide-20031209/wine': {
            u'http://www.w3.org/2000/01/rdf-schema#label': [
            {'type': u'literal', 'value': u'Wine Ontology'}]}})
        
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
        
