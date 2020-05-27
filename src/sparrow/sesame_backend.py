import os
import shutil
import socket
import subprocess
from io import StringIO, BytesIO
from os.path import join

import httplib2
from lxml import etree
from six.moves.urllib_parse import quote, urlencode
from zope.interface import implementer

from sparrow.base_backend import BaseBackend
from sparrow.error import ConnectionError, TripleStoreError, QueryError
from sparrow.interfaces import ITripleStore, ISPARQLEndpoint
from sparrow.utils import (parse_sparql_result,
                           ntriples_to_dict,
                           ntriples_to_json)


@implementer(ITripleStore, ISPARQLEndpoint)
class SesameTripleStore(BaseBackend):

    def __init__(self):
        self._nsmap = {}

    def connect(self, dburi):
        host, rest = dburi[7:].split(':', 1)
        port, self._name = rest.split('/', 1)
        self._url = 'http://%s:%s/openrdf-sesame' % (host, port)
        self._http = httplib2.Http()
        try:
            resp, content = self._http.request(
                '%s/repositories' % self._url,
                "GET",
                headers={'Accept': 'application/sparql-results+xml'})
        except socket.error:
            raise ConnectionError(
                'Can not connect to repository, is it running?')

        if resp['status'] != '200':
            raise ConnectionError(
                'Can not connect to server: %s' % resp['status'])

        for repo in parse_sparql_result(content):
            if repo['id']['value'] == self._name:
                break
        else:
            raise ConnectionError(
                'Server has no repository: %s' % self._name)

    def disconnect(self):
        self._http.close()
        del self._http

    def contexts(self):
        resp, content = self._http.request(
            '%s/repositories/%s/contexts' % (self._url, self._name),
            "GET",
            headers={'Accept': 'application/sparql-results+xml'})

        return [c['contextID']['value'].split(':', 1)[1] for c in
                parse_sparql_result(content)]

    @staticmethod
    def _get_mimetype(format):
        return {'ntriples': 'text/plain',
                'rdfxml': 'application/rdf+xml',
                'turtle': 'application/x-turtle',
                'n3': 'text/rdf+n3',
                'trix': 'application/trix',
                'trig': 'applcation/x-trig'}[format]

    @staticmethod
    def _get_context(context):
        return '<context:%s>' % context

    def register_prefix(self, prefix, namespace):
        # store also in _nsmap for allegro turtle workaround
        self._nsmap[prefix] = namespace

        clength = str(len(namespace))
        resp, content = self._http.request(
            '%s/repositories/%s/namespaces/%s' % (self._url, self._name, prefix),
            'PUT',
            body=namespace,
            headers={"Content-length": clength})
        if resp['status'] != '204':
            raise TripleStoreError(content)

    def add_rdfxml(self, data, context, base_uri):
        data = self._get_file(data)
        self._add(data, 'rdfxml', context, base_uri)

    def add_ntriples(self, data, context):
        data = self._get_file(data)
        self._add(data, 'ntriples', context)

    def add_turtle(self, data, context):
        data = self._get_file(data)
        self._add(data, 'turtle', context)

    def _add(self, file, format, context, base_uri=None):
        data = file.read()
        file.close()
        clength = str(len(data))
        ctype = self._get_mimetype(format)
        params = {'context': self._get_context(context)}
        if base_uri:
            params['baseURI'] = '<%s>' % base_uri
        params = urlencode(params)

        content: bytes
        resp, content = self._http.request(
            '%s/repositories/%s/statements?%s' % (
                self._url, self._name, params),
            'POST',
            body=data,
            headers={"Content-type": ctype,
                     "Content-length": clength})

        if resp['status'] != '204':
            raise TripleStoreError(content)

    def get_rdfxml(self, context):
        return self._serialize('rdfxml', context)

    def get_turtle(self, context):
        return self._serialize('turtle', context)

    def get_ntriples(self, context):
        return self._serialize('ntriples', context)

    def _serialize(self, format, context, pretty=False):

        context = quote(self._get_context(context))
        ctype = self._get_mimetype(format)

        content: bytes
        resp, content = self._http.request(
            '%s/repositories/%s/statements?context=%s' % (
                self._url, self._name, context),
            'GET',
            headers={"Accept": ctype})
        if resp['status'] != '200':
            raise TripleStoreError(content)

        return StringIO(content.decode('utf-8'))

    def remove_rdfxml(self, data, context, base_uri):
        data = self._get_file(data)
        self._remove(data, 'rdfxml', context, base_uri)

    def remove_turtle(self, data, context):
        data = self._get_file(data)
        self._remove(data, 'turtle', context)

    def remove_ntriples(self, data, context):
        data = self._get_file(data)
        self._remove(data, 'ntriples', context)

    def _remove(self, file, format, context, base_uri=None):
        data = file.read()
        file.close()
        clength = str(len(data))
        ctype = self._get_mimetype(format)
        params = {'context': self._get_context(context)}
        if base_uri:
            params['baseURI'] = '<%s>' % base_uri
        params = urlencode(params)
        resp, content = self._http.request(
            '%s/repositories/%s/statements?%s' % (
                self._url, self._name, params),
            'DELETE',
            body=data,
            headers={"Content-type": ctype,
                     "Content-length": clength})
        if resp['status'] != '204':
            raise TripleStoreError(content)

    def clear(self, context):
        context = quote(self._get_context(context))
        resp, content = self._http.request(
            '%s/repositories/%s/statements?context=%s' % (
                self._url, self._name, context),
            'DELETE')
        if resp['status'] != '204':
            raise TripleStoreError(content)

    def count(self, context=None):
        if context is None:
            context = ''
        else:
            context = '?context=' + quote(self._get_context(context))
        resp, content = self._http.request(
            '%s/repositories/%s/size%s' % (
                self._url, self._name, context),
            'GET')
        if resp['status'] != '200':
            raise TripleStoreError(content)
        return int(content)

    def select(self, sparql):
        params = urlencode({'query': sparql,
                            'queryLn': 'SPARQL',
                            'infer': 'false'})

        resp, content = self._http.request(
            '%s/repositories/%s?%s' % (
                self._url, self._name, params),
            'GET',
            headers={'Accept': 'application/sparql-results+xml'})

        if resp['status'] != '200':
            raise QueryError(content)

        # Allegro Graph returns status 200 when parsing failed
        if content.startswith(b'Server error:'):
            raise QueryError(content[14:])

        return parse_sparql_result(content)

    def ask(self, sparql):
        params = urlencode({'query': sparql,
                            'queryLn': 'SPARQL',
                            'infer': 'false'})

        resp, content = self._http.request(
            '%s/repositories/%s?%s' % (
                self._url, self._name, params),
            'GET',
            headers={'Accept': 'application/sparql-results+xml'})

        if resp['status'] != '200':
            raise QueryError(content)
        # Allegro Graph returns status 200 when parsing failed
        if content.startswith(b'Server error:'):
            raise QueryError(content[14:])

        return parse_sparql_result(content)

    def construct(self, sparql, format):
        out_format = format
        if format in ('json', 'dict'):
            out_format = 'ntriples'
        ctype = self._get_mimetype(out_format)
        params = urlencode({
            'query': sparql,
            'queryLn': 'SPARQL',
            'infer': 'false'})

        content: bytes
        resp, content = self._http.request(
            '%s/repositories/%s?%s' % (self._url, self._name, params),
            'GET',
            headers={'Accept': ctype})

        if resp['status'] != '200':
            raise QueryError(content)
        # Allegro Graph returns status 200 when parsing failed
        if content.startswith(b'Server error:'):
            raise QueryError(content[14:])

        if format == 'json':
            return ntriples_to_json(BytesIO(content))
        elif format == 'dict':
            return ntriples_to_dict(BytesIO(content))
        else:
            return StringIO(content.decode('utf-8'))


def start_server(host, port, uri, id, title, path):
    tomcat_dir = join(path, 'parts', 'tomcat-install')

    # check if port and host are configured in tomcats server.xml
    server_conf = join(tomcat_dir, 'conf', 'server.xml')
    doc = etree.parse(server_conf)
    connector_el = doc.xpath('//Service[@name="Catalina"]/Connector')[0]
    if int(connector_el.attrib['port']) != port:
        connector_el.attrib['port'] = str(port)
        doc.write(server_conf)
    engine_el = doc.xpath(
        '//Service[@name="Catalina"]/Engine[@name="Catalina"]')[0]
    if engine_el.attrib['defaultHost'] != host:
        engine_el.attrib['defaultHost'] = host

        host_el = engine_el.xpath('Host')[0]
        if host_el.attrib['name'] != host:
            host_el.attrib['name'] = host
        doc.write(server_conf)

    # copy the war files to tomcat
    sesame_dir = join(path, 'parts', 'sesame-install')
    if not os.path.isfile(join(tomcat_dir, 'webapps', 'openrdf-sesame.war')):
        shutil.copyfile(join(sesame_dir, 'war', 'openrdf-sesame.war'),
                        join(tomcat_dir, 'webapps', 'openrdf-sesame.war'))
    if not os.path.isfile(join(tomcat_dir, 'webapps', 'openrdf-workbench.war')):
        shutil.copyfile(join(sesame_dir, 'war', 'openrdf-workbench.war'),
                        join(tomcat_dir, 'webapps', 'openrdf-workbench.war'))

    catalina = join(tomcat_dir, 'bin', 'catalina.sh')

    os.system('%s run' % catalina)


SESAME_TEST_TEMPLATE = """
drop test.
yes
create memory.
test
Test Repository
false
0
"""

SESAME_MEMORY_TEMPLATE = """
create memory.
%(id)s
%(title)s
false
0
"""

SESAME_NATIVE_TEMPLATE = """
create native.
%(id)s
%(title)s
spoc, posc, opsc
"""

SESAME_MYSQL_TEMPLATE = """
create mysql.
%(id)s
%(title)s
com.mysql.jdbc.Driver
%(host)s
%(port)s
%(db)s
0
%(user)s
%(pwd)s
256
"""


def configure_server(host, port, uri, id, title, path):
    variables = {'id': id, 'title': title}

    if uri == 'memory':
        template = SESAME_MEMORY_TEMPLATE % variables
    elif uri == 'native':
        template = SESAME_NATIVE_TEMPLATE % variables
    elif uri.startswith('mysql://'):
        uri = uri[8:]
        userpart, dbpart = uri.split('@')
        user, pwd = userpart.split(':')
        host, db = dbpart.split('/', 1)
        if ':' in host:
            host, port = host.split(':', 1)
        else:
            port = '3306'
        variables['user'] = user
        variables['pwd'] = pwd
        variables['db'] = db
        variables['host'] = host
        variables['port'] = port
        template = SESAME_MYSQL_TEMPLATE % variables
    else:
        raise ValueError('Unknown Sesame backend URI: %s' % uri)

    conf_script = join(path, 'parts', 'sesame-install', 'bin', 'console.sh')
    proc = subprocess.Popen(conf_script, shell=True, stdin=subprocess.PIPE)
    # connect to sesame
    proc.communicate("connect http://%s:%s/openrdf-sesame." % (host, port) +
                     SESAME_TEST_TEMPLATE +
                     template)
