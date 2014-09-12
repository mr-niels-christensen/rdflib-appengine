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
    '''
    prefixes = ndb.StringProperty(repeated = True)
    namespaces = ndb.StringProperty(repeated = True)

class BNodeSubject(ndb.Model):
    pass

class URIRefSubject(ndb.Model):
    pass

class RdfProperty(ndb.Model):
    pass

class NonLiteralTriple(ndb.Model):
    #Subject is the ancestor^2
    #Property is the immediate ancestor
    triple_object = ndb.KeyProperty()
    
class LiteralTriple(ndb.Model):
    triple_object_lexical = ndb.StringProperty()
    triple_object_datatype = ndb.StringProperty()
    triple_object_language = ndb.StringProperty()

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
        #TODO: Use put_multi
        #TODO: Avoid inserting existing triples?
        for s, p, o, _ in quads:
            parent_key = self._key_for_subject_property(s, p)
            if isinstance(o, term.Literal):
                LiteralTriple(parent = parent_key, 
                              triple_object_lexical = unicode(o),
                              triple_object_datatype = o.datatype,
                              triple_object_language = o.language).put()
            else:
                NonLiteralTriple(parent = parent_key, 
                                 triple_object = self._key_for_entity(o)).put()

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
                .query(ancestor = self._key_for_subject_property(s, p))
                .filter(ndb.AND(LiteralTriple.triple_object_lexical == unicode(o),
                                LiteralTriple.triple_object_datatype == o.datatype,
                                LiteralTriple.triple_object_language == o.language))
                  )
        else:
            query = (
                      NonLiteralTriple
                      .query(ancestor = self._key_for_subject_property(s, p))
                      .filter(NonLiteralTriple.triple_object == self._key_for_entity(o))
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
