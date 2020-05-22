import unittest
import doctest
import datetime
from StringIO import StringIO

FLAGS = doctest.NORMALIZE_WHITESPACE + doctest.ELLIPSIS
GLOBS = {'StringIO': StringIO}

def test_suite():

    suite = unittest.TestSuite()
    suite.addTests([
        doctest.DocFileSuite('README.txt',
                             package='sparrow',
                             globs=GLOBS,
                             optionflags=FLAGS),
        ])

    return suite

