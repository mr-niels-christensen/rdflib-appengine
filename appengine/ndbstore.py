'''
Created on 12 Sep 2014

@author: s05nc4

This code borrows heavily from the Memory Store class by Michel Pelletier, Daniel Krech, Stefan Niederhauser
'''

from rdflib.store import Store
from rdflib import term
from google.appengine.ext import ndb
import hashlib
import logging
from time import time
from rdflib import Graph
from collections import defaultdict
from google.appengine.api import memcache
from rdflib.plugins.memory import IOMemory
from weakref import WeakValueDictionary

ANY = None

def _blank_or_uri(value, is_blank):
    return term.BNode(value = value) if is_blank else term.URIRef(value = value)

def sha1(node):
    #TODO cache hashes?
    m = hashlib.sha1()
    m.update(node.encode('utf-8'))
    return m.hexdigest()

class RdfGraph(ndb.Model):
    '''Use this a ancestor for all triples etc. in the graph
       The entity itself records pairs of (prefix, namespace)
       as two lists of Strings.
    '''
    prefixes = ndb.StringProperty(repeated = True)
    namespaces = ndb.StringProperty(repeated = True)

class NonLiteralTriple(ndb.Model):
    rdf_subject_sha1 = ndb.StringProperty()
    rdf_subject_text = ndb.TextProperty()
    rdf_subject_is_blank = ndb.BooleanProperty()
    rdf_property_sha1 = ndb.StringProperty()
    rdf_property_text = ndb.TextProperty()
    rdf_object_sha1 = ndb.StringProperty()
    rdf_object_text = ndb.TextProperty()
    rdf_object_is_blank = ndb.BooleanProperty()
    
    @staticmethod
    def create(graph_key, s, p, o):
        assert not isinstance(o, term.Literal), "Trying to store a Literal in a NonLiteralTriple: %s %s %s %s" % (graph_key, s, p, o)
        return NonLiteralTriple(parent = graph_key,
                                rdf_subject_sha1 = sha1(s),
                                rdf_subject_text = unicode(s),
                                rdf_subject_is_blank = isinstance(s, term.BNode),
                                rdf_property_sha1 = sha1(p),
                                rdf_property_text = unicode(p),
                                rdf_object_sha1 = sha1(o),
                                rdf_object_text = unicode(o),
                                rdf_object_is_blank = isinstance(o, term.BNode),
                                )
    
        
    def toRdflib(self):
        return (_blank_or_uri(self.rdf_subject_text, self.rdf_subject_is_blank),
                term.URIRef(value = self.rdf_property_text),
                _blank_or_uri(self.rdf_object_text, self.rdf_object_is_blank)
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
        candidate_filters = zip([NonLiteralTriple.rdf_subject_sha1, #We are assuming that BNode names and URIRef names cannot be the same
                                 NonLiteralTriple.rdf_property_sha1, 
                                 NonLiteralTriple.rdf_object_sha1], 
                                [s, p, o])
        filters = [(field == sha1(value)) for field, value in candidate_filters if value is not ANY]
        return NonLiteralTriple.query(ancestor = graph_key).filter(*filters)
    
class LiteralTriple(ndb.Model):
    rdf_subject_sha1 = ndb.StringProperty()
    rdf_subject_text = ndb.TextProperty()
    rdf_subject_is_blank = ndb.BooleanProperty()
    rdf_property_sha1 = ndb.StringProperty()
    rdf_property_text = ndb.TextProperty()
    rdf_object_lexical_sha1 = ndb.StringProperty()
    rdf_object_lexical_text = ndb.TextProperty()
    rdf_object_datatype = ndb.StringProperty()
    rdf_object_language = ndb.StringProperty()
    
    @staticmethod
    def create(graph_key, s, p, o):
        assert isinstance(o, term.Literal), "Trying to store a Literal in a NonLiteralTriple: %s %s %s %s" % (graph_key, s, p, o)
        return LiteralTriple(parent = graph_key, 
                             rdf_subject_sha1 = sha1(s),
                             rdf_subject_text = unicode(s),
                             rdf_subject_is_blank = isinstance(s, term.BNode),
                             rdf_property_sha1 = sha1(p),
                             rdf_property_text = unicode(p),
                             rdf_object_lexical_sha1 = sha1(o),
                             rdf_object_lexical_text = unicode(o),
                             rdf_object_datatype = o.datatype,
                             rdf_object_language = o.language
                             )
        
    def toRdflib(self):
        return (_blank_or_uri(self.rdf_subject_text, self.rdf_subject_is_blank),
                term.URIRef(value = self.rdf_property_text),
                term.Literal(self.rdf_object_lexical_text, 
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
        candidate_filters = zip([LiteralTriple.rdf_subject_sha1,  #We are assuming that BNode names and URIRef names cannot be the same
                                 LiteralTriple.rdf_property_sha1], 
                                [s, p])
        filters = [(field == sha1(value)) for field, value in candidate_filters if value is not ANY]
        if o is not ANY:
            filters += [LiteralTriple.rdf_object_lexical_sha1 == sha1(o),
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
        #Note: quads is a generator, not a list. It cannot be traversed twice.
        triples = [(LiteralTriple if isinstance(o, term.Literal) else NonLiteralTriple).create(self._graph_key, s, p, o) 
                   for (s, p, o, _) in quads]
        ndb.put_multi(triples)

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
        begin = time()
        if o is None or isinstance(o, term.Literal):
            for item in LiteralTriple.matching_query(self._graph_key, s, p, o):
                yield item.toRdflib(), self.__contexts()
        if o is None or not isinstance(o, term.Literal):
            for item in NonLiteralTriple.matching_query(self._graph_key, s, p, o):
                yield item.toRdflib(), self.__contexts()
        logging.debug('{t} seconds used in triples({s},{p},{o})'.format(t = time() - begin, s = s, p = p, o = o))
        
    def __len__(self, context=None): #TODO: Optimize
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        return ( NonLiteralTriple.matching_query(self._graph_key, None, None, None).count()
                 + LiteralTriple.matching_query(self._graph_key, None, None, None).count()
                 )

    def __contexts(self):
        '''Empty generator
        '''
        if False:
            yield

class GraphShard(ndb.Model):
    graph_n3 = ndb.TextProperty(compressed = True)
    #TODO add date for cleaning up
    graph_ID = ndb.StringProperty()

    _graph_cache = WeakValueDictionary()
    _not_found = set() #Because None or object() cannot be weakref'ed
    
    def rdflib_graph(self):
        g = GraphShard._graph_cache.get(self._parsed_memcache_key(), GraphShard._not_found)
        if g is not GraphShard._not_found:
            return g
        g = memcache.get(self._parsed_memcache_key())
        if g is not None:
            GraphShard._graph_cache[self._parsed_memcache_key()] = g
            return g
        g = Graph(store = IOMemory())
        g.parse(data = self.graph_n3, format='n3')
        GraphShard._graph_cache[self._parsed_memcache_key()] = g
        memcache.add(self._parsed_memcache_key(), g, 86400)
        return g
    
    def _parsed_memcache_key(self):
        return 'IOMemory({})'.format(self.key.id())
    
    @staticmethod
    def invalidate(instances):
        for instance in instances:
            GraphShard._graph_cache[instance._parsed_memcache_key()] = GraphShard._not_found
        memcache.delete_multi([instance._parsed_memcache_key() for instance in instances])
        
    @staticmethod
    def key_for(graph_ID, uri_ref, index):
        assert index in range(3), 'index was {}, must be one of 0 for subject, 1 for predicate, 2 for object'.format(index)
        if index == 1: #Keep predicates completely separate
            wiff = uri_ref.split('/')[-1].replace('-','')
            if len(wiff) > 20:
                wiff = wiff[-20:]
            uri_ref_digest = '{}_{}'.format(wiff, sha1(uri_ref))
        else: #Split into 16
            uri_ref_digest = sha1(uri_ref)[-1]
        return ndb.Key(GraphShard, '{}-{}-{}-{}'.format('spo'[index], uri_ref_digest, '', graph_ID))

    def spo(self):
        return self.key.id().split('-')[0]
    
    @staticmethod
    def keys_for(graph_ID, uri_ref, index):
        return [GraphShard.key_for(graph_ID, 
                                   uri_ref,
                                   index)]

class CoarseNDBStore(Store):
    """
    A triple store using NDB on GAE (Google App Engine)
    """
    
    def __init__(self, configuration=None, identifier=None):
        super(CoarseNDBStore, self).__init__(configuration)
        assert identifier is not None, "CoarseNDBStore requires a basestring identifier"
        assert isinstance(identifier, basestring), "CoarseNDBStore requires a basestring identifier"
        assert len(identifier) > 0, "CoarseNDBStore requires a non-empty identifier"
        assert len(identifier) < 64, "CoarseNDBStore requires a brief identifier"
        self._ID = identifier

    def addN(self, quads):
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        #Note: quads is a generator, not a list. It cannot be traversed twice.
        new_shard_dict = defaultdict(Graph)
        #TODO: Handle splitting large graphs into two entities
        for (s, p, o, _) in quads:
            new_shard_dict[GraphShard.key_for(self._ID, s, 0)].add((s, p, o))
            new_shard_dict[GraphShard.key_for(self._ID, p, 1)].add((s, p, o))
        keys = list(new_shard_dict.keys())
        keys_models = zip(keys, ndb.get_multi(keys)) #TODO: Use async get
        updated = list()
        for index in range(len(keys_models)):
            (key, model) = keys_models[index]
            if model is None:
                model = GraphShard(key = key, graph_ID = self._ID, graph_n3 = new_shard_dict[key].serialize(format='n3'))
            else:
                new_shard_dict[key].parse(data = model.graph_n3, format='n3')
                model.graph_n3 = new_shard_dict[key].serialize(format='n3')
            updated.append(model)
        if len(updated) > 0:
            GraphShard.invalidate(updated)
            ndb.put_multi(updated)

    def add(self, (subject, predicate, o), context, quoted=False):
        """\
        Redirects to addN() because NDB heavily favours batch updates.
        """
        logging.warn('Inefficient usage: 1 triple being added')
        self.addN([(subject, predicate, o, context)])

    def remove(self, (s, p, o), context=None):
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        graph_shards = ndb.get_multi(GraphShard.keys_for(self._ID, s, 0)) + ndb.get_multi(GraphShard.keys_for(self._ID, p, 1))
        updated = []
        for m in graph_shards:
            if m is not None:
                g = m.rdflib_graph()
                g.remove((s, p, o))
                m.graph_n3 = g.serialize(format='n3')
                updated.append(m)
        if len(updated) > 0:
            GraphShard.invalidate(updated)
            ndb.put_multi(updated)

    def triples(self, (s, p, o), context=None):
        """A generator over all the triples matching """
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        begin = time()
        logging.debug('{}: triples({}, {}, {})'.format(begin, s, p, o))
        if p == ANY:
            if s == ANY:
                models = self._all_predicate_shard_models()
                pattern = (s, p, o)
            else:
                models = ndb.get_multi(GraphShard.keys_for(self._ID, s, 0))
                pattern = (s, p, o) #IOMemory is slower if you provide a redundant binding
        else:
            models = ndb.get_multi(GraphShard.keys_for(self._ID, p, 1))
            pattern = (s, ANY, o) #IOMemory is slower if you provide a redundant binding
        for m in models:
            if m is not None:
                #logging.debug('BEGIN traversing {}'.format(m.key))
                #hits = 0
                #pred = None
                g = m.rdflib_graph()
                #logging.debug('PARSED {}'.format(m.key))
                for t in g.triples(pattern): #IOMemory is slower if you provide a redundant binding
                    #hits += 1
                    #pred = t[1]
                    yield t, self.__contexts()
                #logging.debug('END traversing {}, found {} hits, pred={}'.format(m.key, hits, pred))
        logging.debug('{}: done'.format(begin))

    def _all_predicate_shard_models(self):
        logging.warn('Inefficient usage: Traversing all triples')
        for m in GraphShard.query().filter(GraphShard.graph_ID == self._ID).iter():
            if m is not None and m.spo() == 'p':
                yield m
                
    def __len__(self, context=None): #TODO: Optimize
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        logging.warn('Inefficient usage: __len__'.format())
        return sum([len(m.rdflib_graph()) for m in self._all_predicate_shard_models()])

    def __contexts(self):
        '''Empty generator
        '''
        if False:
            yield
