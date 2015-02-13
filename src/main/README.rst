rdflib-appengine
================

This aim of this project is to allow rdflib, the Python RDF library, to be deployed on Google App Engine (GAE) with data be persisted in the NDB storage.

All use is at your own risk.

To use this project your application should run on GAE using Python 2.7.

Assuming that you are familiar with rdflib (https://rdflib.readthedocs.org/en/latest/), to get started with this project, simply create a graph like this:

.. code:: python

  from rdflib import Graph
  from rdflib_appengine.ndbstore import NDBStore
  g = Graph(store = NDBStore(identifier = 'my_first_store'))

See https://github.com/mr-niels-christensen/rdflib-appengine and https://semanticwebrecipes.wordpress.com/2015/01/09/triple-store-in-the-cloud/ for further information.
