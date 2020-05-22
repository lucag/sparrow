import os
from unittest import TestCase, TestSuite, makeSuite, main

import sparrow
from sparrow.error import ConnectionError
from sparrow.tests.base_tests import (TripleStoreTest,
                                      TripleStoreQueryTest,
                                      open_test_file)


# To run these tests, make sure the allegro.cfg buildout profile is used
# this will export a 'ALLEGRO_HOST' and 'ALLEGRO_PORT' variable to
# the environ dict which is used by this test to determine where
# the allegro server is running.

# The allegro server will need to be configured with a repository called
# 'test' this repository can be a simple in memory database.
# Run the configure_allegro tool to generate the test repository, and add the
# working repository as specified in the buildout profile

# Note that the state of the database can get messed up if tests fail or
# the tests are cancelled. You can clear the whole database with the following
# command:
# curl -X DELETE "http://localhost:8001/sesame/repositories/test/statements"
#
# If you get connection errors, you might want to create a directory 'test'
# in the directory where you started the allegro server

class AllegroTest(TripleStoreTest):
    def setUp(self):
        self.db = sparrow.database('allegro', get_allegro_url())

    def tearDown(self):
        self.db.clear('test')
        self.db.disconnect()
        del self.db

class AllegroQueryTest(TripleStoreQueryTest):
    def setUp(self):
        self.db = sparrow.database('allegro', get_allegro_url())
        fp = open_test_file('ntriples')
        self.db.add_ntriples(fp, 'test')
        fp.close()

    def tearDown(self):
        self.db.clear('test')
        self.db.disconnect()
        del self.db
    
def get_allegro_url():
    # host and port variables are set from
    # the buildout script (see profiles/allegro.cfg)
    url = 'http://%s:%s/test' % (os.environ.get('ALLEGRO_HOST', 'localhost'),
                                 os.environ.get('ALLEGRO_PORT', '8000'))
    
    return url

def test_suite():
    try:
        sparrow.database('allegro', get_allegro_url())
    except ConnectionError, err:
        # allegro not running?
        return TestSuite()

    suite = TestSuite()
    suite.addTest(makeSuite(AllegroTest))
    suite.addTest(makeSuite(AllegroQueryTest))
    return suite

if __name__ == '__main__':

    main(defaultTest='test_suite')
