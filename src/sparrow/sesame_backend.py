import os
import shutil
import subprocess
from io import StringIO, BytesIO
from os.path import join
from urllib.parse import urlparse, quote, urlencode

import requests
from lxml import etree
from zope.interface import implementer

from sparrow.base_backend import BaseBackend
from sparrow.error import ConnectionError, TripleStoreError, QueryError
from sparrow.interfaces import ITripleStore, ISPARQLEndpoint
from sparrow.utils import (parse_sparql_result,
                           ntriples_to_dict,
                           ntriples_to_json)


def to_bytes(response: requests.Response) -> bytes:
    with BytesIO() as f:
        for chunk in response.iter_content(chunk_size=128):
            f.write(chunk)
        return f.getvalue()


@implementer(ITripleStore, ISPARQLEndpoint)
class SesameTripleStore(BaseBackend):

    def __init__(self):
        self._nsmap = {}
        self._name = self._url = None

    def connect(self, dburi):
        url = urlparse(dburi)
        self._name = url.path[1:]  # Remove slash
        self._url = 'http://%s/openrdf-sesame' % url.netloc
        try:
            resp = requests.get(
                f'{self._url}/repositories',
                headers={'Accept': 'application/sparql-results+xml'})
        except requests.ConnectionError as err:
            raise ConnectionError(f'Got {err} while connecting to repository; is it running?')

        if resp.status_code != 200:
            raise ConnectionError('Can not connect to server: %d' % resp.status_code)

        for repo in parse_sparql_result(to_bytes(resp)):
            if repo['id']['value'] == self._name:
                break
        else:
            raise ConnectionError('Server has no repository: %s' % self._name)

    def disconnect(self):
        pass

    def contexts(self):
        resp = requests.get(
            f'{self._url}/repositories/{self._name}/contexts',
            headers={'Accept': 'application/sparql-results+xml'})

        return [c['contextID']['value'].split(':', 1)[1]
                for c in parse_sparql_result(to_bytes(resp))]

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

        content_len = str(len(namespace))
        resp = requests.put(
            f'{self._url}/repositories/{self._name}/namespaces/{prefix}',
            data=namespace,
            headers={"Content-length": content_len})

        if resp.status_code != 204:
            raise TripleStoreError(resp)

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

        resp = requests.post(
            f'{self._url}/repositories/{self._name}/statements?{urlencode(params)}',
            data=data,
            headers={"Content-type": ctype,
                     "Content-length": clength})

        if resp.status_code != 204:
            raise TripleStoreError(resp)

    def get_rdfxml(self, context):
        return self._serialize('rdfxml', context)

    def get_turtle(self, context):
        return self._serialize('turtle', context)

    def get_ntriples(self, context):
        return self._serialize('ntriples', context)

    def _serialize(self, format, context, pretty=False):

        context = quote(self._get_context(context))
        ctype = self._get_mimetype(format)

        resp = requests.get(
            f'{self._url}/repositories/{self._name}/statements?context={context}',
            headers={"Accept": ctype})

        if resp.status_code != 200:
            raise TripleStoreError(resp)
        else:
            return StringIO(resp.text)

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
        content = requests.delete(
            f'{self._url}/repositories/{self._name}/statements?{params}',
            data=data,
            headers={"Content-type": ctype,
                     "Content-length": clength})

        if content.status_code != 204:
            raise TripleStoreError(content)

    def clear(self, context):
        context = quote(self._get_context(context))
        resp = requests.delete(
            f'{self._url}/repositories/{self._name}/statements?context={context}')

        if resp.status_code != 204:
            raise TripleStoreError(resp)

    def count(self, context=None):
        context = '?context=' + quote(self._get_context(context)) if context else ''
        resp = requests.get(f'{self._url}/repositories/{self._name}/size{context}')

        if resp.status_code != 200:
            raise TripleStoreError(resp)
        else:
            return int(resp.text)

    def select(self, sparql):
        params = urlencode({'query': sparql,
                            'queryLn': 'SPARQL',
                            'infer': 'false'})

        content = requests.get(
            f'{self._url}/repositories/{self._name}?{params}',
            headers={'Accept': 'application/sparql-results+xml'})

        if content.status_code != 200:
            raise QueryError(content.status_code)

        # Allegro Graph returns status 200 when parsing failed
        if content.text.startswith('Server error:'):
            raise QueryError(content[14:])

        return parse_sparql_result(to_bytes(content))

    def ask(self, sparql):
        params = urlencode({'query': sparql,
                            'queryLn': 'SPARQL',
                            'infer': 'false'})

        resp = requests.get(
            f'{self._url}/repositories/{self._name}?{params}',
            headers={'Accept': 'application/sparql-results+xml'})

        if resp.status_code != 200:
            raise QueryError(resp.status_code)

        # Allegro Graph returns status 200 when parsing failed
        if resp.text.startswith('Server error:'):
            raise QueryError(resp[14:])

        return parse_sparql_result(to_bytes(resp))

    def construct(self, sparql, fmt):
        out_format = fmt
        if fmt in ('json', 'dict'):
            out_format = 'ntriples'
        ctype = self._get_mimetype(out_format)
        params = urlencode({
            'query': sparql,
            'queryLn': 'SPARQL',
            'infer': 'false'})

        # content: bytes
        resp = requests.get(
            f'{self._url}/repositories/{self._name}?{params}',
            headers={'Accept': ctype})

        if resp.status_code != 200:
            raise QueryError(resp.status_code)

        # Allegro Graph returns status 200 when parsing failed
        if resp.text.startswith('Server error:'):
            raise QueryError(resp[14:])

        if fmt == 'json':
            return ntriples_to_json(BytesIO(to_bytes(resp)))
        elif fmt == 'dict':
            return ntriples_to_dict(BytesIO(to_bytes(resp)))
        else:
            return StringIO(resp.text)


def start_server(host, port, uri, id, title, path):
    tomcat_dir = join(path, 'parts', 'tomcat-install')

    # check if port and host are configured in tomcats server.xml
    server_conf = join(tomcat_dir, 'conf', 'server.xml')
    doc = etree.parse(server_conf)
    connector_el = doc.xpath('//Service[@name="Catalina"]/Connector')[0]
    if int(connector_el.attrib['port']) != port:
        connector_el.attrib['port'] = str(port)
        doc.write(server_conf)

    engine_el = doc.xpath('//Service[@name="Catalina"]/Engine[@name="Catalina"]')[0]
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
