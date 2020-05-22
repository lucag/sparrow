import os
from unittest import TestCase, TestSuite, makeSuite, main

import sparrow
from sparrow.error import ConnectionError
from sparrow.tests.base_tests import (TripleStoreTest,
                                      TripleStoreQueryTest,
                                      open_test_file)


# To run these tests, make sure the sesame.cfg buildout profile is used
# this will export a 'SESAME_HOST' and 'SESAME_PORT' variable to
# the environ dict which is used by this test to determine where
# the sesame server is running.

# The sesame server will need to be configured with a repository called
# 'test' this repository can be a simple in memory database.
# Run the configure_sesame tool to generate the test repository, and add the
# working repository as specified in the buildout profile

class SesameTest(TripleStoreTest):
    def setUp(self):
        self.db = sparrow.database('sesame', get_sesame_url())

    def tearDown(self):
        self.db.clear('test')
        self.db.disconnect()
        del self.db

class SesameQueryTest(TripleStoreQueryTest):
    def setUp(self):
        self.db = sparrow.database('sesame', get_sesame_url())
        fp = open_test_file('ntriples')
        self.db.add_ntriples(fp, 'test')
        fp.close()
        
    def tearDown(self):
        self.db.clear('test')
        self.db.disconnect()
        del self.db
    
def get_sesame_url():
    # host and port variables are set from
    # the buildout script (see profiles/sesame.cfg)
    url = 'http://%s:%s/test' % (os.environ.get('SESAME_HOST', 'localhost'),
                                 os.environ.get('SESAME_PORT', '8000'))
    return url

def test_suite():
    try:
        sparrow.database('sesame', get_sesame_url())
    except ConnectionError:
        # sesame not running?
        return TestSuite()

    suite = TestSuite()
    suite.addTest(makeSuite(SesameTest))
    suite.addTest(makeSuite(SesameQueryTest))
    return suite

if __name__ == '__main__':

    main(defaultTest='test_suite')
