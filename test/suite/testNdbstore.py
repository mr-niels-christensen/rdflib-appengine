import unittest
from appengine import ndbstore
from google.appengine.ext import testbed
from rdflib.term import URIRef, Literal
import itertools

_BIG_URIREF = URIRef('http://%s' % ('x' * 500))
_BIG_LITERAL = Literal('x' * 1500)

_TRIPLES = [(s, p, o) for [s, p, o] in itertools.product([URIRef('http://s%d' % i) for i in range(2)] + [_BIG_URIREF],
                                                         [URIRef('http://p%d' % i) for i in range(2)] + [_BIG_URIREF],
                                                         [URIRef('http://o')] 
                                                         #+ [Literal(x) for x in [2, 3.14, 'ba#na.na']]
                                                         + [_BIG_URIREF]
                                                         #+ [_BIG_LITERAL],
                                                         )]

class TestCase(unittest.TestCase):
    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()

    def testConstructor(self):
        ndbstore.NDBStore(identifier = 'banana')
        
    def testSingleAddAndRetrieve(self):
        st = ndbstore.NDBStore(identifier = 'banana')
        st.add(_TRIPLES[0], None)
        self.assertEquals(1, len(st))
        self._assertSameSet(_TRIPLES[0:1], st.triples((None, None, None), None))

    def testSequenceAddAndRetrieveAndRemove(self):
        st = ndbstore.NDBStore(identifier = 'banana')
        for index in range(len(_TRIPLES)):
            st.add(_TRIPLES[index], None)
            self.assertEquals(1, len(st))
            self._assertSameSet(_TRIPLES[index:index+1], st.triples((None, None, None), None))
            st.remove(_TRIPLES[index], None)
            self.assertEquals(0, len(st))
            self._assertSameSet([], st.triples((None, None, None), None))
    
    def testAddN(self):
        st = ndbstore.NDBStore(identifier = 'banana')
        st.addN([(s, p, o, None) for (s, p, o) in _TRIPLES])
        self.assertEquals(len(_TRIPLES), len(st))
        self._assertSameSet(_TRIPLES, st.triples((None, None, None), None))
    
    def testTriples(self):
        st = ndbstore.NDBStore(identifier = 'banana')
        st.addN([(s, p, o, None) for (s, p, o) in _TRIPLES])
        patterns = itertools.product(*zip(_TRIPLES[0], _TRIPLES[-1], [None, None, _TRIPLES[0][2]]))#None]))
        for pattern in patterns:
            self._assertSameMatches(st, pattern)
        
    def _assertSameMatches(self, st, (s, p, o)):
        mine = _TRIPLES
        if s is not None:
            mine = [t for t in mine if t[0] == s]
        if p is not None:
            mine = [t for t in mine if t[1] == p]
        if o is not None:
            mine = [t for t in mine if t[2] == o]
        self._assertSameSet(mine, st.triples((s, p, o), None))
        
    def _assertSameSet(self, triple_list, quad_generator):
        self.assertEquals(set(triple_list), set([t for (t, _) in quad_generator]))
        
if __name__ == '__main__':
    unittest.main()
