import unittest
import doctest
import datetime
from six.moves import StringIO

FLAGS = doctest.NORMALIZE_WHITESPACE + doctest.ELLIPSIS
GLOBS = {'StringIO': StringIO}

def test_suite():

    suite = unittest.TestSuite()
    suite.addTests([
        doctest.DocFileSuite('../../README.rst',
                             package='sparrow',
                             globs=GLOBS,
                             optionflags=FLAGS),
        ])

    return suite

