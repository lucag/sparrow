from io import BytesIO
from unittest import TestCase, TestSuite, makeSuite, main, mock

from sparrow.tests.utils import to_tuple, ANY
from sparrow.utils import ntriples_to_dict, dict_to_ntriples


class DictFormatTest(TestCase):
    def test_uri_object(self):
        nt = b'<uri:a> <uri:b> <uri:c> .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertEqual(data, {
            u'uri:a': {
                u'uri:b': [
                    {'value': 'uri:c', 'type': 'uri'}
                ]
            }
        })
        self.assertEqual(nt, dict_to_ntriples(data).read())

    def test_bnode_object(self):
        nt = b'<uri:a> <uri:b> _:c .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertDictEqual(
            {'uri:a': {'uri:b': [{'value': mock.ANY, 'type': 'bnode'}]}},
            data
        )
        self.assertEqual(nt, dict_to_ntriples(data, 'c').read())

    def test_bnode_subject(self):
        nt = b'_:a <uri:b> _:c .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertSequenceEqual(to_tuple({
            ANY: {'uri:b': [{'value': ANY, 'type': 'bnode'}]}
        }), to_tuple(data))
        self.assertEqual(nt, dict_to_ntriples(data, 'a', 'c').read())

    def test_literal_object(self):
        nt = b'<uri:a> <uri:b> "foo" .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertEqual(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'foo',
                                    'type': u'literal'}]}})
        self.assertEqual(nt, dict_to_ntriples(data).read())

    def test_literal_language_object(self):
        nt = b'<uri:a> <uri:b> "foo"@en .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertEqual(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'foo',
                                    'lang': u'en',
                                    'type': u'literal'}]}})
        self.assertEqual(nt, dict_to_ntriples(data).read())

    def test_literal_datatype_object(self):
        nt = b'<uri:a> <uri:b> "foo"^^<uri:string> .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertEqual(
            {'uri:a': {'uri:b': [{'value': 'foo',
                                  'datatype': 'uri:string',
                                  'type': 'literal'}]}},
            data
        )
        self.assertEqual(nt, dict_to_ntriples(data).read())

    def test_literal_value_quote_escape(self):
        nt = b'<uri:a> <uri:b> "I say \\"Hello\\"." .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertEqual(
            data,
            {u'uri:a': {u'uri:b': [{'value': u'I say "Hello".',
                                    'type': u'literal'}]}})
        self.assertEqual(nt, dict_to_ntriples(data).read())

    def test_literal_value_backslash_escape(self):
        nt = b'<uri:a> <uri:b> "c:\\\\temp\\\\foo.txt" .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertDictEqual(
            {'uri:a': {'uri:b': [{'value': 'c:\\temp\\foo.txt',
                                  'type': 'literal'}]}},
            data)
        self.assertEqual(nt, dict_to_ntriples(data).read())

    def test_literal_value_newline_tab_escape(self):
        nt = b'<uri:a> <uri:b> "\\n\\tHello!\\n" .\n'
        data = ntriples_to_dict(BytesIO(nt))
        self.assertDictEqual(
            {u'uri:a': {u'uri:b': [{'value': u'\n\tHello!\n',
                                    'type': u'literal'}]}},
            data)
        self.assertEqual(nt, dict_to_ntriples(data).read())


def test_suite():
    suite = TestSuite()
    suite.addTest(makeSuite(DictFormatTest))
    return suite


if __name__ == '__main__':
    main(defaultTest='test_suite')
