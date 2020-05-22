from unittest import TestCase, TestSuite, makeSuite, main
from StringIO import StringIO

from sparrow.utils import ntriples_to_dict, dict_to_ntriples

class DictFormatTest(TestCase):
    def test_uri_object(self):
        nt = '<uri:a> <uri:b> <uri:c> .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'uri:c',
                                    'type': u'uri'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    def test_bnode_object(self):
        nt = '<uri:a> <uri:b> _:c .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'c',
                                    'type': u'bnode'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    def test_bnode_subject(self):
        nt = '_:a <uri:b> _:c .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'_:a': {u'uri:b': [{'value': u'c',
                                  'type': u'bnode'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    def test_literal_object(self):
        nt = '<uri:a> <uri:b> "foo" .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'foo',
                                    'type': u'literal'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    def test_literal_language_object(self):
        nt = '<uri:a> <uri:b> "foo"@en .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'foo',
                                    'lang': u'en',
                                    'type': u'literal'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    def test_literal_datatype_object(self):
        nt = '<uri:a> <uri:b> "foo"^^<uri:string> .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'foo',
                                    'datatype': u'uri:string',
                                    'type': u'literal'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    def test_literal_value_quote_escape(self):
        nt = '<uri:a> <uri:b> "I say \\"Hello\\"." .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'I say "Hello".',
                                    'type': u'literal'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())
        
    def test_literal_value_backslash_escape(self):
        nt = '<uri:a> <uri:b> "c:\\\\temp\\\\foo.txt" .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'c:\\temp\\foo.txt',
                                    'type': u'literal'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    def test_literal_value_newline_tab_escape(self):
        nt = '<uri:a> <uri:b> "\\n\\tHello!\\n" .\n'
        data = ntriples_to_dict(StringIO(nt))
        self.assertEquals(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'\n\tHello!\n',
                                    'type': u'literal'}]}})
        self.assertEquals(nt, dict_to_ntriples(data).read())

    
def test_suite():
    suite = TestSuite()
    suite.addTest(makeSuite(DictFormatTest))
    return suite

if __name__ == '__main__':
    main(defaultTest='test_suite')
