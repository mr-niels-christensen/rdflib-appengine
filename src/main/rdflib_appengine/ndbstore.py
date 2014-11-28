'''
Created on 12 Sep 2014

@author: Niels Christensen
A triple store using NDB on GAE (Google App Engine)

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
    '''@param node: Some basestring or rdflib.term, encodable in UTF-8
       @return: The hex SHA1 of the given string
    '''
    m = hashlib.sha1()
    m.update(node.encode('utf-8'))
    return m.hexdigest()

class GraphShard(ndb.Model):
    '''Stores a subset of all triples in a Graph
       A single GraphShard will typically contain triples for one specific predicate
       or a selection of subjects.
       A GraphShard can be retrieved quickly and cheaply by its key.
       The triples are stored in the N3 format, gzip compressed by NDB.
    '''
    graph_n3 = ndb.TextProperty(compressed = True)
    graph_ID = ndb.StringProperty()

    '''A cache for previously retrieved GraphShard that haven't yet been garbage collected.
       This is important because all joins will be performed lazily, which means the query evaluator
       will ask for the same triples over and over again within milliseconds.'''
    _graph_cache = WeakValueDictionary()
    _not_found = set() #Placeholder used in _graph_cache because None or object() cannot be weakref'ed

    '''Retrieve an rdflib.Graph() containing the triples in this GraphShard.
       This method searches three layers of store in order:
         * a local WeakValueDictionary containing rdflib.Graph() objects (very, very fast)
         * The Memcache provided by NDB containing pickled rdflib.Graph() objects (needs unpickling, takes 0-300ms in my experience)
         * NDB itself containing compressed N3 data (needs retrieval, decompression and parsing, takes 0-1500ms in my experience)
       The method stores the returned object in the two first layers before returning.
       @return The rdflib.Graph() containing the triples in this GraphShard.
    '''    
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
        '''@return The key used for caching self.rdflib_graph() in Memcache and
           the internal _graph_cache
        '''
        return 'IOMemory({})'.format(self.key.id())
    
    @staticmethod
    def invalidate(instances):
        '''Removes all cached copies of the given GraphShard instances.
           @param instances: A collection of GraphShard instances
        '''
        for instance in instances:
            GraphShard._graph_cache[instance._parsed_memcache_key()] = GraphShard._not_found
        memcache.delete_multi([instance._parsed_memcache_key() for instance in instances])
        
    @staticmethod
    def key_for(graph_ID, uri_ref, index):
        '''Assemble an NDB key for the GraphShard containing triples relevant to the given parameters.
           @param graph_ID: The name of the graph to get triples from, e.g. 'current'
           @param uri_ref: The rdflib.URIRef to get triples for
           @param index: 0, 1 or 2 to indicate at which position the triples should have the given uri_ref.
                         0=subject, 1=predicate, 2=object
           @return An ndb.Key for the relevant GraphShard
        '''
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
        '''Checks whether this GraphShard stores triples for specific subjects, predicates, or objects.
           @return either 's', 'p' or 'o'.
        '''
        return self.key.id().split('-')[0]
    
    @staticmethod
    def keys_for(graph_ID, uri_ref, index):
        '''Like key_for() but returns a list of ndb.Keys
           This method was made to prepare for the situation where there will be more than
           one GraphShard for the parameters. Sorry for the confusion.
        '''
        return [GraphShard.key_for(graph_ID, 
                                   uri_ref,
                                   index)]

'''Default configuration parameter to NDBStores constructor'''
_STD_CONFIG = {'log' : False}

class NDBStore(Store):
    """
    A triple store using NDB on GAE (Google App Engine)
    
    Every triple is stored in 2 GraphShards. For example, (URIRef('http://s'), URIRef('http://p'), Literal(42)) will be stored in:
      * The GraphShard containing every triple with http://p as the predicate
      * The GraphShard containing every triple with a subject that hashes to the same as http://s
    The hash used for a subject is the last digit of the subject's hex SHA1. 
    
    An NDBStore contains an internal log to which it writes information about SPARQL query execution
    and calls to triples(). This information can be logged by calling flush_log().
    If you do not wish to use memory for this log, set configuration to {'log': False} in the constructor.
    
    This implementation heavily favours
      * batch updates, i.e. using addN() with many triples
      * triple() queries where either subject or predicate is bound
      * not asking for the length of the NDBStore
      
    This module registers a custom SPARQL query evaluator that
      * Writes the parsed form of every SELECT query to the internal log
      * Performs all joins as lazy joins, which is much faster for NDBStore in my experience.
    """
    
    def __init__(self, configuration=_STD_CONFIG, identifier=None):
        '''@param configuration: A dict mapping 'log' to True or False
           @param identifier: A nonempty string or unicode. It's length must be <64
           to keep internal keys reasonably small.
        '''
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
        '''Add a message to this objects internal log.
           @param msg: The message, a string. It may contain newlines.
        '''
        if self._is_logging:
            self._log.write('\n{:.3f}s: '.format(time() - self._log_begin))
            self._log.write(msg)

    def flush_log(self, level):
        '''Logs all stored log messages on the given level and clears the internal log.
           @level: The (integer) level from the logging module, e.g. logging.DEBUG 
        '''
        if self._is_logging:
            logging.log(level, self._log.getvalue())
            self._log = StringIO()
        
    def addN(self, quads):
        #TODO: Handle splitting large graphs into two entities
        #Note: quads is a generator, not a list. It cannot be traversed twice.
        #Step 1: Collect the triples into the Graphs reflecting the GraphShards they will be added to.
        new_shard_dict = defaultdict(Graph)
        for (s, p, o, _) in quads: #Last component ignored as this Store is not context_aware
            new_shard_dict[GraphShard.key_for(self._ID, s, 0)].add((s, p, o))
            new_shard_dict[GraphShard.key_for(self._ID, p, 1)].add((s, p, o))
        keys = list(new_shard_dict.keys())
        #Step 2: Load all existing, corresponding GraphShards
        keys_models = zip(keys, ndb.get_multi(keys)) #TODO: Use async get
        #Step 3: Update or create GraphShards with the added triples
        updated = list()
        for index in range(len(keys_models)):
            (key, model) = keys_models[index]
            if model is None:
                model = GraphShard(key = key, graph_ID = self._ID, graph_n3 = new_shard_dict[key].serialize(format='n3'))
            else:
                new_shard_dict[key].parse(data = model.graph_n3, format='n3')
                model.graph_n3 = new_shard_dict[key].serialize(format='n3')
            updated.append(model)
        #Step 4: Invalidate and store all created/updated GraphShards
        if len(updated) > 0:
            GraphShard.invalidate(updated)
            ndb.put_multi(updated)

    def add(self, (subject, predicate, o), context, quoted=False):
        """\
        Redirects to addN() because NDB heavily favours batch updates.
        """
        logging.warn('Inefficient use: 1 triple being added')
        self.addN([(subject, predicate, o, context)])

    def remove(self, (s, p, o), context=None):
        #Step 1: Get all relevant GraphShards
        graph_shards = ndb.get_multi(GraphShard.keys_for(self._ID, s, 0)) + ndb.get_multi(GraphShard.keys_for(self._ID, p, 1))
        #Step 2: Remove the given triple from the found GraphShards
        updated = []
        for m in graph_shards:
            if m is not None:
                g = m.rdflib_graph()
                g.remove((s, p, o))
                m.graph_n3 = g.serialize(format='n3')
                updated.append(m)
        #Step 3: Invalidate and store the updated GraphShards
        if len(updated) > 0:
            GraphShard.invalidate(updated)
            ndb.put_multi(updated)

    def triples(self, (s, p, o), context=None):
        #Log execution data using a random ID
        log_id = '{:04d}'.format(randrange(1000))
        self.log('{} triples({}, {}, {})'.format(log_id, s, p, o))
        #Analyse bindings to see if the query can be answered using a single GraphShard
        if p == ANY:
            if s == ANY:
                #(s,p,o) == (ANY,ANY,o), so all GraphShards must be consulted
                models = self._all_predicate_shard_models()
                pattern = (s, p, o)
            else:#s is bound so only the GraphShard for s (and subjects with same hash) needs to be consulted
                models = ndb.get_multi(GraphShard.keys_for(self._ID, s, 0))
                pattern = (s, p, o)
        else:#p is bound so only the GraphShard for p needs to be consulted
            models = ndb.get_multi(GraphShard.keys_for(self._ID, p, 1))
            pattern = (s, ANY, o) #Remove p because IOMemory is slower if you provide a redundant binding
        for m in models:
            if m is not None:
                g = m.rdflib_graph()
                for t in g.triples(pattern):
                    yield t, self.__contexts()
        self.log('{} done'.format(log_id))

    def _all_predicate_shard_models(self):
        '''Generator yielding every GraphShard for the identified graph.
        '''
        logging.warn('Inefficient usage: Traversing all triples')
        for m in GraphShard.query().filter(GraphShard.graph_ID == self._ID).iter():
            if m is not None and m.spo() == 'p': #Avoid yield each triple twice (once for each GraphShard it is stored in)
                yield m
                
    def __len__(self, context=None):
        return sum([len(m.rdflib_graph()) for m in self._all_predicate_shard_models()])

    def __contexts(self):
        '''Empty generator
        '''
        if False:
            yield
# ------------------------------------------------------------------------
# The following defines the custom SPARQL query evaluator for Graphs backed by an NDBStore
 
def _evalPartWithLoggingAndLazyJoins(ctx, part):
    '''Supplement to rdflib.plugins.sparql.evaluate.evalPart().
       Only active when ctx.graph is backed by an NDBStore
       Dumps any SELECT query to the NDBStores internal log.
       Executes every join as a lazy join.
    '''
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
    '''Pretty printer for a SPARQL query parsed by rdflib.
       The query will be written on multiple lines.
       @param part: Part of a parsed SPARQL query
       @param indent: A string that is prepended to every lines printed for this part of the query
       @param dest: A file-like object to which the output is written
    '''
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

'''Register the above SPARQL query evaluator in rdflib'''
CUSTOM_EVALS['ndbstore'] = _evalPartWithLoggingAndLazyJoins
logging.info('Activated specialized query evaluation in rdflib')
