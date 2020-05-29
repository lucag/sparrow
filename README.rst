
Sparrow
=======

Foreword abd Disclaimer
-----------------------

This is an old library that I maintain for an old project. I updated the dependencies
and modified it to run on Python 3.6+. Only `rdflib`_ and `RDF4J`_ have been tested;
`Allegro Graph`_ is not working and I have no interest in making it work at this time. Also,
I haven't pushed it to `PyPI`_ — please feel free to do so if interested. Credit for the
library should go to the original implementor.

.. _RDF4J: http://www.rdf4j.com
.. _PyPI: https://pypi.org

Introduction
------------

Sparrow is a library that provides a high-level API for
RDF Databases. Any database the provides support for SPARQL Queries and
has a triplestore that can handle contexts can be used as a backend.

The goal of Sparrow is to make sure all these different backends act the same,
making it possible to change RDF Database backends without having to change
your code.

At the moment there is support for the following backends:

 * `Redland / librdf`_
 * `RDFLib`_
 * `Sesame / openrdf`_
 * `Allegro Graph`_

.. _Redland / librdf: http://librdf.org
.. _RDFLib: http://www.rdflib.net
.. _Sesame / openrdf: http://www.openrdf.org
.. _Allegro Graph: http://www.franz.com/agraph/

The API provides support for the following basic functions:

 * Parsing RDF statements from different formats into a contextual database
 * Serializing the RDF statements from a specific context of a database
 * Removing statements from a specific context of a database
 * Performing SPARQL Queries

Sparrow does not provide a Graph API, but it can parse and serialize python
dictionary objects. This uses the same format as the JSON serialization.
Each TripleStore backend provides the following formats:
 
 * RDFXML 
 * NTriples
 * Turtle
 * JSON
 * Python dictionaries


Installation
------------

Sparrow comes with buildout profiles for several databases. 
These profiles will install and setup the different backends for you.
You don't have to use buildout, but I would recommend it.

To install you need `Pipenv`_. Having installed the latter, type:


::

  > pipenv -dev -e .

This will create some scripts in the bin folder like a testrunner and
(depending on which profile you choose) scripts for configuring and starting
the different backends.

.. _Pipenv: https://pipenv-fork.readthedocs.io/en/latest/

Usage
-----

Normally, you will only need to import the base sparrow module

>>> import sparrow

Most of the database backends will not work out of the box. 
Since the RDFLib backend is written in python and packaged on pypi,
it is always available, and installed with Sparrow.

Let's create an in memory rdflib database

>>> db = sparrow.database('rdflib', 'memory')
>>> db
<sparrow.rdflib_backend.RDFLibTripleStore ...>

Let's add some triples to the database, we will use turtle syntax for this.
We'll make some example statements where we will state that john is a person,
and that his firstname is "John".

>>> data = """@prefix ex: <http://example.org#> .
... ex:john a ex:Person; ex:name "John" ."""

Now we can add this to the database. We will need to tell the database in 
which context to store the data. The data itself can be either a file or http
based URI, a string of data, or a file-like object.

So, let's add this to the `persons` context.

>>> db.add_turtle(StringIO(data), 'persons')

We can now ask the database, which contexts it has:

>>> db.contexts()
['persons']

You can store data in as many different contexts as you like, or put everything
in a single context.

Lets do a simple SPARQL query on the database

>>> result = db.select('SELECT ?x {?x <http://example.org#name> "John".}')

We can get the results as a list of dictionaries. This follows the SPARQL
JSON result format.

>>> assert result == [{'x': {'type': 'uri', 'value': 'http://example.org#john'}}]

Besides querying, we can also get the data back from the database in any
of the supported formats. We specify which format we want, and which context
to use.

>>> db.get_ntriples('persons').read()
'<http://example.org#john> ...'

If the database backend supports it, you can ask how many triples are in a 
context.

>>> db.count('persons')
2

If you want to remove triples, you will need to supply data describing which
triples to remove.

>>> data = '<http://example.org#john> a <http://example.org#Person>.'
>>> db.remove_turtle(data, 'persons')
>>> db.count('persons')
1

You can also remove all triples in a context

>>> db.clear('persons')
>>> db.count('persons')
0

Since the 'persons' context is now empty, it is also removed.

>>> db.contexts()
[]

