import unittest
from appengine import ndbstore
from google.appengine.ext import testbed
from rdflib.term import URIRef, Literal
import itertools

_TRIPLES = [(s, p, o) for [s, p, o] in itertools.product([URIRef('http://s%d' % i) for i in range(2)],
                                                         [URIRef('http://p%d' % i) for i in range(2)],
                                                         [URIRef('http://o')] 
                                                         + [Literal(x) for x in [2, 3.14, 'ba#na.na']],
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
        
    def testSingleStoreAndRetrieve(self):
        s = ndbstore.NDBStore(identifier = 'banana')
        s.add(_TRIPLES[0], None)
        self.assertEquals(1, len(s))
        self.assertEquals(_TRIPLES[0:1], [t for (t, _) in s.triples((None, None, None), None)])

if __name__ == '__main__':
    unittest.main()
