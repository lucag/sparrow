from __future__ import print_function

from unittest import TestSuite, main, TestLoader

import sparrow
from sparrow.error import ConnectionError
from sparrow.tests.base_tests import (TripleStoreTest,
                                      TripleStoreQueryTest,
                                      open_test_file)


class RDFLibTest(TripleStoreTest):
    def setUp(self):
        super(RDFLibTest, self).setUp()
        self.db = sparrow.database('rdflib', 'memory')

    def tearDown(self):
        super(RDFLibTest, self).tearDown()
        self.db.disconnect()
        del self.db


class RDFLibQueryTest(TripleStoreQueryTest):
    def setUp(self):
        super(RDFLibQueryTest, self).setUp()
        self.db = sparrow.database('rdflib', 'memory')
        with open_test_file('ntriples') as fp:
            self.db.add_ntriples(fp, 'test')

    def tearDown(self):
        super(RDFLibQueryTest, self).tearDown()
        self.db.disconnect()
        del self.db


# See: http://codereview.stackexchange.com/q/88655/15346
def make_suite(*tc_classes):
    tests = [test for tc in tc_classes for test in TestLoader().loadTestsFromTestCase(tc)]
    return tests

def test_suite():
    try:
        sparrow.database('rdflib', 'memory')
    except ConnectionError:
        print('rdflib not installed?')
        return TestSuite()
    suite = TestSuite()
    suite.addTests(make_suite(RDFLibTest, RDFLibQueryTest))
    return suite


if __name__ == '__main__':
    main(defaultTest='test_suite')
