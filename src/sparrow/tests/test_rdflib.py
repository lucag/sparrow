from unittest import TestCase, TestSuite, makeSuite, main

import sparrow
from sparrow.error import ConnectionError
from sparrow.tests.base_tests import (TripleStoreTest,
                                      TripleStoreQueryTest,
                                      open_test_file)

class RDFLibTest(TripleStoreTest):
    def setUp(self):
        self.db = sparrow.database('rdflib', 'memory')

    def tearDown(self):
        self.db.disconnect()
        del self.db

class RDFLibQueryTest(TripleStoreQueryTest):
    def setUp(self):
        self.db = sparrow.database('rdflib', 'memory')
        fp = open_test_file('ntriples')
        self.db.add_ntriples(fp, 'test')
        fp.close()
        
    def tearDown(self):
        self.db.disconnect()
        del self.db

def test_suite():
    try:
        sparrow.database('rdflib', 'memory')
    except ConnectionError:
        # rdflib not installed?
        return TestSuite()
    suite = TestSuite()
    suite.addTest(makeSuite(RDFLibTest))
    suite.addTest(makeSuite(RDFLibQueryTest))
    return suite

if __name__ == '__main__':
    main(defaultTest='test_suite')
