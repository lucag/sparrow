import codecs
from typing import List, Any, Tuple, Union

import simplejson
from io import BytesIO, StringIO
from lxml import etree

from sparrow import ntriples

SPARQLNS = u'http://www.w3.org/2005/sparql-results#'


def parse_sparql_result(xml):
    doc = etree.fromstring(xml)
    results = []
    for result in doc.xpath('/s:sparql/s:results/s:result', namespaces={'s': SPARQLNS}):
        data = {}
        for binding in result:
            name = binding.attrib['name']
            for value in binding:
                type = value.tag.split('}')[-1]
                lang = value.attrib.get('{http://www.w3.org/XML/1998/namespace}lang')
                datatype = value.attrib.get('datatype')
                text = value.text
                if text is not None and not isinstance(text, str):
                    text = text.decode('utf8')

                if type == 'uri':
                    # allegro graph returns context uri's with <> chars
                    if text.startswith('<'):
                        text = text[1:]
                    if text.endswith('>'):
                        text = text[:-1]

                data[name] = {'value': text,
                              'type': type}
                if not lang is None:
                    # w3c sparql json result spec says 'xml:lang',
                    # we use 'lang' instead, just like the dict serialization
                    # data[name]['lang'] = lang.decode('utf8')
                    data[name]['lang'] = lang
                if not datatype is None:
                    # data[name]['datatype'] = datatype.decode('utf8')
                    data[name]['datatype'] = datatype
                    # w3c sparql json result spec says 'type' should not be
                    # 'literal', but 'typed-literal', we don't do this

                    # data[name]['type'] = 'typed-literal'

        results.append(data)
    if not results:
        # maybe this is an ASK query response
        bools = doc.xpath('/s:sparql/s:boolean/text()',
                          namespaces={'s': SPARQLNS})
        if bools:
            if bools[0] == 'true':
                return True
            else:
                return False
    return results


def ntriples_to_dict(file):
    """This needs a byte stream
    """
    class TripleDict(dict):
        def triple(self, s, p, o):
            if isinstance(s, ntriples.URI):
                subject = s.toPython()
            elif isinstance(s, ntriples.bNode):
                subject = u'_:%s' % s.toPython()
            else:
                raise ValueError('Unknown subject type: %s' % type(s))
            predicate = p.toPython()
            predicates = self.get(subject, {})
            values = predicates.get(predicate, [])
            value = {'value': str(o)}
            if isinstance(o, ntriples.URI):
                value['type'] = 'uri'
            elif isinstance(o, ntriples.bNode):
                value['type'] = 'bnode'
            elif isinstance(o, ntriples.Literal):
                o: ntriples.Literal
                lang, dtype, literal = o.language, o.datatype, str(o)
                value['type'] = 'literal'
                value['value'] = literal
                if lang:
                    value['lang'] = lang
                elif dtype:
                    value['datatype'] = dtype.toPython()
            else:
                raise ValueError('Unknown object type: %s' % type(o))

            values.append(value)
            self[subject] = predicates
            predicates[predicate] = values

    parser = ntriples.NTriplesParser(TripleDict())
    # result = parser.parse(BytesIO(file.read().encode('utf-8')))
    result = parser.parse(file)
    return dict(result)


def to_bytes(data: str) -> bytes:
    return bytes(data.encode('utf-8'))

def dict_to_ntriples(data, *bnodes):
    # result = StringIO()
    def translate(data):
        for subject, predicates in data.items():
            if not subject.startswith('_:'):
                subject = '<%s>' % subject
            for predicate, values in predicates.items():
                predicate = '<%s>' % predicate
                for value in values:
                    if value['type'] == 'uri':
                        obj = f'<{value["value"]}>'
                    elif value['type'] == 'bnode':
                        obj = f'_:{value["value"]}'
                    else:
                        obj = value['value']
                        obj = obj.replace('\\', '\\\\')
                        obj = obj.replace('\t', '\\t')
                        obj = obj.replace('\n', '\\n')
                        obj = obj.replace('\r', '\\r')
                        obj = obj.replace('\"', '\\"')
                        obj = f'"{obj}"'
                        if value.get('lang'):
                            obj = f'{obj}@{(value["lang"])}'
                        elif value.get('datatype'):
                            obj = '%s^^<%s>' % (obj, (value['datatype']))
                    # result.write(to_bytes('%s %s %s .\n' % (subject, predicate, obj)))
                    yield subject, predicate, obj
                    # result.write('%s %s %s .\n' % (subject, predicate, obj))

    def take(n, xs):
        it = iter(xs)
        for _ in range(n):
            yield next(it)

    def group(n, it):
        yield tuple(take(3, it))

    def as_byte_stream(ts):
        return BytesIO(b'\n'.join(to_bytes('%s %s %s .\n' % t) for t in ts))

    if bnodes:
        # Expand bnodes
        bn = iter(bnodes)
        maybe_subst = lambda x: f'_:{next(bn)}' if x.startswith('_:') else x
        ts = group(3, [maybe_subst(x) for t in translate(data) for x in t])
        return as_byte_stream(ts)
    else:
        return as_byte_stream(translate(data))


def json_to_ntriples(data):
    data = simplejson.load(data)
    return dict_to_ntriples(data)


def ntriples_to_json(triples):
    data = ntriples_to_dict(triples)
    return StringIO(simplejson.dumps(data, indent=True))
