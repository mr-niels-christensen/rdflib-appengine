'''
Created on 12 Sep 2014

@author: s05nc4

This code borrows heavily from the Memory Store class by Michel Pelletier, Daniel Krech, Stefan Niederhauser
'''

from rdflib.store import Store
from rdflib import term
from google.appengine.ext import ndb

ANY = Any = None

def _blank_or_uri(value, is_blank):
    return term.BNode(value = value) if is_blank else term.URIRef(value = value)

class RdfGraph(ndb.Model):
    '''Use this a ancestor for all triples etc. in the graph
       The entity itself records pairs of (prefix, namespace)
       as two lists of Strings.
    '''
    prefixes = ndb.StringProperty(repeated = True)
    namespaces = ndb.StringProperty(repeated = True)

class NonLiteralTriple(ndb.Model):
    rdf_subject = ndb.StringProperty()
    rdf_subject_is_blank = ndb.BooleanProperty()
    rdf_property = ndb.StringProperty()
    rdf_object = ndb.StringProperty()
    rdf_object_is_blank = ndb.BooleanProperty()
    
    @staticmethod
    def create(graph_key, s, p, o):
        assert not isinstance(o, term.Literal), "Trying to store a Literal in a NonLiteralTriple: %s %s %s %s" % (graph_key, s, p, o)
        return NonLiteralTriple(parent = graph_key, 
                                rdf_subject = unicode(s),
                                rdf_subject_is_blank = isinstance(s, term.BNode),
                                rdf_property = unicode(p),
                                rdf_object = unicode(o),
                                rdf_object_is_blank = isinstance(o, term.BNode),
                                )
    
        
    def toRdflib(self):
        return (_blank_or_uri(self.rdf_subject, self.rdf_subject_is_blank),
                term.URIRef(value = self.rdf_property),
                _blank_or_uri(self.rdf_object, self.rdf_object_is_blank)
                )
    
    @staticmethod
    def matching_query(graph_key, s, p, o):
        '''Combines subject, property, nonliteral object into an NDB query.
           The query is not executed.
           @param s RDF subject or None (for wildcard)
           @param p RDF property or None (for wilcard)
           @param o Nonliteral RDF object or None (for wildcard)
           @return An NDB query that will find the matching triples when executed.
        '''
        candidate_filters = zip([NonLiteralTriple.rdf_subject, #We are assuming that BNode names and URIRef names cannot be the same
                                 NonLiteralTriple.rdf_property, 
                                 NonLiteralTriple.rdf_object], 
                                [s, p, o])
        filters = [(field == value) for field, value in candidate_filters if value is not None]
        return NonLiteralTriple.query(ancestor = graph_key).filter(*filters)
    
class LiteralTriple(ndb.Model):
    rdf_subject = ndb.StringProperty()
    rdf_subject_is_blank = ndb.BooleanProperty()
    rdf_property = ndb.StringProperty()
    rdf_object_lexical = ndb.StringProperty()
    rdf_object_datatype = ndb.StringProperty()
    rdf_object_language = ndb.StringProperty()
    
    @staticmethod
    def create(graph_key, s, p, o):
        assert isinstance(o, term.Literal), "Trying to store a Literal in a NonLiteralTriple: %s %s %s %s" % (graph_key, s, p, o)
        return LiteralTriple(parent = graph_key, 
                             rdf_subject = unicode(s),
                             rdf_subject_is_blank = isinstance(s, term.BNode),
                             rdf_property = unicode(p),
                             rdf_object_lexical = unicode(o)[0:400],#TODO Store data
                             rdf_object_datatype = o.datatype,
                             rdf_object_language = o.language
                             )
        
    def toRdflib(self):
        return (_blank_or_uri(self.rdf_subject, self.rdf_subject_is_blank),
                term.URIRef(value = self.rdf_property),
                term.Literal(self.rdf_object_lexical, 
                             lang = self.rdf_object_language, 
                             datatype = self.rdf_object_datatype)
                )    
    
    @staticmethod
    def matching_query(graph_key, s, p, o):
        '''Combines subject, property, literal object into an NDB query.
           The query is not executed.
           @param s RDF subject or None (for wildcard)
           @param p RDF property or None (for wilcard)
           @param o Literal RDF object or None (for wildcard)
           @return An NDB query that will find the matching triples when executed.
        '''
        candidate_filters = zip([NonLiteralTriple.rdf_subject,  #We are assuming that BNode names and URIRef names cannot be the same
                                 NonLiteralTriple.rdf_property], 
                                [s, p])
        filters = [(field == value) for field, value in candidate_filters if value is not None]
        if o is not None:
            filters += [LiteralTriple.rdf_object_lexical == unicode(o),
                        LiteralTriple.rdf_object_datatype == o.datatype,
                        LiteralTriple.rdf_object_language == o.language]
        return LiteralTriple.query(ancestor = graph_key).filter(*filters)

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
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        #Convert rdflib tirples to NDB triples
        lit_triples = [LiteralTriple.create(self._graph_key, s, p, o) 
                       for (s, p, o, _) in quads if isinstance(o, term.Literal)]
        nonlit_triples = [NonLiteralTriple.create(self._graph_key, s, p, o)
                          for (s, p, o, _) in quads if not isinstance(o, term.Literal)]
        #Insert all the triples in one operation because NDB may allow only one write operation per graph per second. 
        #See bottom of https://developers.google.com/appengine/docs/python/datastore/structuring_for_strong_consistency
        ndb.put_multi(lit_triples + nonlit_triples)

    def add(self, (subject, predicate, o), context, quoted=False):
        """\
        Redirects to addN() because NDB heavily favours batch updates.
        """
        self.addN([(subject, predicate, o, context)])

    def remove(self, (s, p, o), context=None):
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        if isinstance(o, term.Literal):
            query = LiteralTriple.matching_query(self._graph_key, s, p, o)
        else:
            query = NonLiteralTriple.matching_query(self._graph_key, s, p, o)
        for key in query.iter(keys_only = True):
            key.delete()

    def triples(self, (s, p, o), context=None):
        """A generator over all the triples matching """
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        if o is None or isinstance(o, term.Literal):
            for item in LiteralTriple.matching_query(self._graph_key, s, p, o):
                yield item.toRdflib(), self.__contexts()
        if o is None or not isinstance(o, term.Literal):
            for item in NonLiteralTriple.matching_query(self._graph_key, s, p, o):
                yield item.toRdflib(), self.__contexts()
        
    def __len__(self, context=None): #TODO: Optimize
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        return ( NonLiteralTriple.matching_query(self._graph_key, None, None, None).count()
                 + LiteralTriple.matching_query(self._graph_key, None, None, None).count()
                 )

    def bind(self, prefix, namespace): #TODO is namespace allowed to be None?
        self._graph.prefixes = [prefix] + self._graph.prefixes
        self._graph.namespaces = [prefix] + self._graph.namespaces
        self._graph.put()

    def namespace(self, prefix):
        try: 
            return self._graph.namespaces[self._graph.prefixes.index(prefix)]
        except ValueError:
            return None

    def prefix(self, namespace):
        try: 
            return self._graph.prefixes[self._graph.namespaces.index(namespace)]
        except ValueError:
            return None

    def namespaces(self):
        for prefix, namespace in zip(self._graph.prefixes, self._graph.namespaces):
            yield prefix, namespace

    def __contexts(self):
        '''Empty generator
        '''
        if False:
            yield
