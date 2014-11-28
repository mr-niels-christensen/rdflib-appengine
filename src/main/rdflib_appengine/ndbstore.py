'''
Created on 12 Sep 2014

@author: s05nc4

This code borrows heavily from the Memory Store class by Michel Pelletier, Daniel Krech, Stefan Niederhauser
'''

from rdflib.store import Store
from google.appengine.ext import ndb
import hashlib
import logging
from time import time
from rdflib import Graph
from collections import defaultdict
from google.appengine.api import memcache
from rdflib.plugins.memory import IOMemory
from weakref import WeakValueDictionary
from StringIO import StringIO
from random import randrange
from rdflib.plugins.sparql.evaluate import evalLazyJoin
from rdflib.plugins.sparql import CUSTOM_EVALS

ANY = None #Convention used by rdflib

def sha1(node):
    m = hashlib.sha1()
    m.update(node.encode('utf-8'))
    return m.hexdigest()

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

_STD_CONFIG = {'log' : False}

class NDBStore(Store):
    """
    A triple store using NDB on GAE (Google App Engine)
    """
    
    def __init__(self, configuration=_STD_CONFIG, identifier=None):
        super(NDBStore, self).__init__(configuration)
        assert identifier is not None, "NDBStore requires a basestring identifier"
        assert isinstance(identifier, basestring), "NDBStore requires a basestring identifier"
        assert len(identifier) > 0, "NDBStore requires a non-empty identifier"
        assert len(identifier) < 64, "NDBStore requires a brief identifier"
        self._ID = identifier
        self._log = StringIO()
        self._log_begin = time()
        assert isinstance(configuration['log'], bool), "Configuration must set 'log' to True or False"
        self._is_logging = configuration['log']

    def log(self, msg):
        if self._is_logging:
            self._log.write('\n{:.3f}s: '.format(time() - self._log_begin))
            self._log.write(msg)

    def flush_log(self, level):
        if self._is_logging:
            logging.log(level, self._log.getvalue())
            self._log = StringIO()
        
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
        log_id = '{:04d}'.format(randrange(1000))
        self.log('{} triples({}, {}, {})'.format(log_id, s, p, o))
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
                g = m.rdflib_graph()
                for t in g.triples(pattern): #IOMemory is slower if you provide a redundant binding
                    yield t, self.__contexts()
        self.log('{} done'.format(log_id))

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
# ------------------------------------------------------------------------
# Modify query evaluation with NDBStore in rdflib
 
def _evalPartWithLoggingAndLazyJoins(ctx, part):
    if not isinstance(ctx.graph.store, NDBStore):
        raise NotImplementedError
    if part.name == 'SelectQuery':
        s = StringIO()
        _dump(part, '', s)
        ctx.graph.store.log(s.getvalue())
        raise NotImplementedError
    elif part.name == 'Join':
        return evalLazyJoin(ctx, part)
    else:
        raise NotImplementedError

def _dump(part, indent, dest):
    if part is None:
        return None
    if part.name == 'BGP':
        dest.write('{}{} {:04d} triples={}\n'.format(indent, part.name, id(part) % 10000, part.triples))
        return
    if part.name == 'Extend':
        dest.write('{}{} {:04d} {}={}\n'.format(indent, part.name, id(part) % 10000, part.var, part.expr))
    else:
        dest.write('{}{} {:04d}\n'.format(indent, part.name, id(part) % 10000))
    for attr in ['p', 'p1', 'p2']:
        if hasattr(part, attr):
            child  = getattr(part, attr)
            if child is not None:
                _dump(child, '{}  '.format(indent), dest)
    return

CUSTOM_EVALS['ndbstore'] = _evalPartWithLoggingAndLazyJoins
logging.info('Activated specialized query evaluation in rdflib')
