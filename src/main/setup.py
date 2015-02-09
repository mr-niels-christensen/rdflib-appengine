#!/usr/bin/env python

from distutils.core import setup

def readme():
    with open('README.rst') as f:
        return f.read()

setup(name='rdflib-appengine',
      version = '1.2',
      description='Python distributible for using rdflib with NDB',
      long_description=readme(),
      author='Niels Christensen',
      packages=['rdflib_appengine',
                ],
      install_requires=[
          'rdflib==4.1.2',
      ],
     )