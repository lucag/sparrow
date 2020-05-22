from unittest import TestCase, TestSuite, makeSuite, main

import sparrow
from sparrow.error import ConnectionError
from sparrow.tests.base_tests import (TripleStoreTest,
                                      TripleStoreQueryTest,
                                      open_test_file)

class RedlandTest(TripleStoreTest):
    def setUp(self):
        self.db = sparrow.database('redland', 'memory')

    def tearDown(self):
        self.db.disconnect()
        del self.db

class RedlandQueryTest(TripleStoreQueryTest):
    def setUp(self):
        self.db = sparrow.database('redland', 'memory')
        self.db.add_ntriples(open_test_file('ntriples'), 'test')
        
    def tearDown(self):
        self.db.disconnect()
        del self.db

def test_suite():
    try:
        sparrow.database('redland', 'memory')
    except ConnectionError:
        # redland / librdf not installed?
        return TestSuite()
    suite = TestSuite()
    suite.addTest(makeSuite(RedlandTest))
    suite.addTest(makeSuite(RedlandQueryTest))
    return suite

if __name__ == '__main__':
    main(defaultTest='test_suite')
