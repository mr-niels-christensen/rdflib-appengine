#!/usr/bin/env python

from setuptools import setup

def readme():
    with open('./README.rst') as f:
        return f.read()

setup(name='rdflib-appengine',
      version = '1.2.0',
      description='Python distributible for using rdflib with NDB',
      long_description=readme(),
      url='https://github.com/mr-niels-christensen/rdflib-appengine',
      author='Niels Christensen',
      author_email='nhc@mayacs.com',
      license='Apache 2.0',
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Developers',
                   'Topic :: Software Development :: Libraries',
                   'License :: OSI Approved :: Apache Software License',
                   'Programming Language :: Python :: 2.7'],
      keywords='semantic-web rdf rdflib google-app-engine gae ndb',
      packages=['rdflib_appengine',
               ],
      data_files = ['README.rst'],
      install_requires=[
          'rdflib==4.1.2',
      ],
     )