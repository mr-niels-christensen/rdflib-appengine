'''
Created on 13 Nov 2014

@author: Niels Christensen

This code borrows heavily from the Memory Store class by Michel Pelletier, Daniel Krech, Stefan Niederhauser
'''

from rdflib.store import Store
from rdflib import Graph
from google.appengine.ext import ndb
import hashlib
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from google.appengine.api import memcache
from rdflib.plugins.memory import IOMemory

ANY = None

def sha1(node):
    #TODO cache hashes?
    m = hashlib.sha1()
    m.update(node.encode('utf-8'))
    return m.hexdigest()
    
class GraphShard(ndb.Model):
    graph_n3 = ndb.TextProperty(compressed = True)
    #TODO add date for cleaning up
    graph_ID = ndb.StringProperty()

    def rdflib_graph(self):
        key = 'IOMemory({})'.format(self.key.id())
        g = memcache.get(key)
        if g is not None:
            return g
        g = Graph(store = IOMemory())
        g.parse(data = self.graph_n3, format='n3')
        memcache.add(key, g, 600)
        return g
    
    @staticmethod
    def key_for(graph_ID, uri_ref, index):
        #TODO: Maybe only SHA1 if uri_ref is long
        assert index in range(3), 'index was {}, must be one of 0 for subject, 1 for predicate, 2 for object'.format(index)
        return ndb.Key(GraphShard, '{}-{}-{}'.format(sha1(uri_ref), 'spo'[index], graph_ID))

    @staticmethod
    def keys_for(graph_ID, uri_ref, index):
        return [GraphShard.key_for(graph_ID, 
                                   uri_ref,
                                   index)]

def _today_as_isostring(delta = timedelta(0)):
    return (datetime.utcnow().date() - delta).strftime('%Y-%m-%d')

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
            ndb.put_multi(updated)

    def add(self, (subject, predicate, o), context, quoted=False):
        """\
        Redirects to addN() because NDB heavily favours batch updates.
        """
        logging.warn('Inefficient usage: 1 triple being added')
        self.addN([(subject, predicate, o, context)])

    def remove(self, (s, p, o), context=None):
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        graph_shards = ndb.get_multi(GraphShard.keys_for(self._ID, p, 1))
        updated = []
        for m in graph_shards:
            if m is not None:
                g = m.rdflib_graph()
                g.remove((s, p, o))
                m.graph_n3 = g.serialize(format='n3')
                updated.append(m)
        if len(updated) > 0:
            ndb.put_multi(updated)

    def triples(self, (s, p, o), context=None):
        """A generator over all the triples matching """
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        from time import time
        begin = time()
        logging.debug('{}: triples({}, {}, {})'.format(begin, s, p, o))
        if p == ANY:
            logging.warn('Inefficient usage: p is None in {}'.format((s, p, o)))
            models = GraphShard.query().filter(GraphShard.graph_ID == self._ID).iter()
        else:
            models = ndb.get_multi(GraphShard.keys_for(self._ID, p, 1)) 
        logging.debug('{}: RPC done'.format(begin))
        for m in models:
            if m is not None:
                for t in m.rdflib_graph().triples((s, ANY, o)):
                    yield t, self.__contexts()
        logging.debug('{}: done'.format(begin))

        
    def __len__(self, context=None): #TODO: Optimize
        #TODO: What is the meaning of the supplied context? I got [a rdfg:Graph;rdflib:storage [a rdflib:Store;rdfs:label 'NDBStore']]
        logging.warn('Inefficient usage: __len__'.format())
        return sum([len(m.rdflib_graph()) for m in GraphShard.query().filter(GraphShard.graph_ID == self._ID).fetch()])

    def __contexts(self):
        '''Empty generator
        '''
        if False:
            yield
