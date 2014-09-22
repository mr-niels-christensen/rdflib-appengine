import unittest
from appengine import ndbstore
from google.appengine.ext import testbed
from google.appengine.datastore import datastore_stub_util

class TestCase(unittest.TestCase):
    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        self.testbed. init_datastore_v3_stub()
        self.testbed.init_memcache_stub()

    def tearDown(self):
        self.testbed.deactivate()

    def testConstructor(self):
        s = ndbstore.NDBStore(identifier = 'banana')

if __name__ == '__main__':
    unittest.main()
