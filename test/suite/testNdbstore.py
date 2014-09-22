import unittest
from appengine import ndbstore
from google.appengine.ext import testbed
from rdflib.term import URIRef, Literal

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
        
    def testSimpleStoreAndRetrieve(self):
        s = ndbstore.NDBStore(identifier = 'banana')
        t = (URIRef('http://s'), URIRef('http://p'), Literal('o'))
        s.add(t, None)
        self.assertEquals(1, len(s))
        self.assertEquals([t], [t for (t, _) in s.triples((None, None, None), None)])

if __name__ == '__main__':
    unittest.main()
