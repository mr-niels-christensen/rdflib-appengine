'''
Created on 12 Sep 2014

@author: s05nc4

This code borrows heavily from the Memory Store class by Michel Pelletier, Daniel Krech, Stefan Niederhauser
'''

from rdflib.store import Store
from rdflib import term
from google.appengine.ext import ndb

ANY = Any = None

class RdfGraph(ndb.Model):
    '''Use this a ancestor for all triples etc. in the graph
       The entity itself records pairs of (prefix, namespace)
       as two lists of Strings.
    '''
    prefixes = ndb.StringProperty(repeated = True)
    namespaces = ndb.StringProperty(repeated = True)

class NonLiteralTriple(ndb.Model):
    rdf_subject = ndb.StringProperty()
    rdf_property = ndb.StringProperty()
    rdf_object = ndb.StringProperty()
    
class LiteralTriple(ndb.Model):
    rdf_subject = ndb.StringProperty()
    rdf_property = ndb.StringProperty()
    rdf_object_lexical = ndb.StringProperty()
    rdf_object_datatype = ndb.StringProperty()
    rdf_object_language = ndb.StringProperty()

class NDBStore(Store):
    """\
    A triple store using NDB on GAE (Google App Engine)
    """
    def __init__(self, configuration=None, identifier=None):
        super(NDBStore, self).__init__(configuration)
        assert identifier is not None, "NDBStore requires a basestring identifier"
        assert isinstance(identifier, basestring), "NDBStore requires a basestring identifier"
        assert len(identifier) > 0, "NDBStore requires a non-empty identifier"
        self._graph_key = ndb.Key(RdfGraph, identifier)
        self._graph = self._graph_key.get() 
        if self._graph is None:
            self._graph = RdfGraph(key = self._graph_key, prefixes = [], namespaces = [])
            self._graph.put()

    def addN(self, quads):
        for s, p, o, c in quads:
            assert c is None, \
                "Context associated with %s %s %s is not None but %s!" % (s, p, o, c)
        #Convert rdflib tirples to NDB triples
        lit_triples = [LiteralTriple(parent = self._graph_key, 
                                     rdf_subject = unicode(s),
                                     rdf_property = unicode(p),
                                     rdf_object_lexical = unicode(o),
                                     rdf_object_datatype = o.datatype,
                                     rdf_object_language = o.language) 
                       for (s, p, o, _) in quads if isinstance(o, term.Literal)]
        nonlit_triples = [NonLiteralTriple(parent = self._graph_key, 
                                           rdf_subject = unicode(s),
                                           rdf_property = unicode(p),
                                           rdf_object = unicode(o)) 
                          for (s, p, o, _) in quads if isinstance(o, term.Literal)]
        #Insert all the triples in one operation because NDB may allow only one write operation per graph per second. 
        #See bottom of https://developers.google.com/appengine/docs/python/datastore/structuring_for_strong_consistency
        ndb.put_multi(lit_triples + nonlit_triples)

    def add(self, (subject, predicate, o), context, quoted=False):
        """\
        Redirects to addN() because NDB heavily favours batch updates.
        """
        self.addN([(subject, predicate, o, context)])

    def remove(self, (s, p, o), context=None):
        assert context is None, "Context not supported"
        if isinstance(o, term.Literal):
            query = (
                LiteralTriple
                .query(ancestor = self._graph_key)
                .filter(ndb.AND(LiteralTriple.rdf_object_lexical == unicode(o),
                                LiteralTriple.rdf_object_datatype == o.datatype,
                                LiteralTriple.rdf_object_language == o.language))
                  )
        else:
            query = (
                      NonLiteralTriple
                      .query(ancestor = self._graph_key)
                      .filter(NonLiteralTriple.triple_object == unicode(o))
                      )
        for key in query.run(keys_only = True):
            key.delete()

    def triples(self, (s, p, o), context=None):
        """A generator over all the triples matching """
        assert False, "Not implemented yet"
        

    def __len__(self, context=None):
        assert False, "Not implemented yet"

    def bind(self, prefix, namespace):
        assert False, "Not implemented yet"

    def namespace(self, prefix):
        assert False, "Not implemented yet"

    def prefix(self, namespace):
        assert "Not implemented yet"

    def namespaces(self):
        assert False, "Not implemented yet"

    def __contexts(self):
        return (c for c in [])  # TODO: best way to return empty generator
