# -*- coding: utf-8 -*-
import unittest
from rdflib_appengine import ndbstore
from google.appengine.ext import testbed
from rdflib.term import URIRef, Literal
from rdflib import Graph
from uuid import uuid4
from random import sample

_TRIPLES = [(URIRef('http://subject'), 
             URIRef('http://predicate'), 
             Literal('''The Joking Computer can create millions of new jokes such as "What do you get when you cross a frog with a street? A main toad." It enthuses the general public, and children in particular, for computing science, explains the computational humour research on which it is based, and involves the public in the on-going research (by rating jokes and indicating where they disagree with the Joking Computer's reasoning). The Joking Computer public engagement project built upon a previous EPSRC-funded research project that created a computer program (STANDUP) that allowed children with disabilities such as cerebral palsy to create novel jokes. The aim of STANDUP was to provide a motivating environment for these children to develop their linguistic knowledge. The Joking Computer project used this work to create a public exhibit, which enables children to explore language and humour in an engaging fashion, while also finding out about the mechanisms used by the computer to create jokes and being able to help the scientists to improve the joke building. The exhibits were installed in Glasgow Science Centre (Dec 2009 – August 2010), Satrosphere Aberdeen (March 2010 – present), and soon in Dundee Sensations Science Centre (May 2012 onwards). These have been used by thousands of members of the general public. Workshops with groups of schoolchildren were run at the Word Festival (May 2010), Techfest (September 2010), and Satrosphere (October 2010). The materials created for these workshops have been made generally available for primary school teachers to use, and have received enthusiastic approval from the teachers who attended our workshops. There have been presentations about the work at the International Summer School on Humour and Laughter (Switzerland 2010,  Estonia 2011), the 2nd International Conference on Computational Creativity (Mexico 2011), the Autumn School on Computational Creativity (Helsinki 2011), University of Caen (France 2012),  Cafe Scientifique (Aberdeen 2010).  The last of these was a talk for the general public rather than an academic event. A child-friendly website was created, with a large amount of educational information and some interactive games, including a full-scale interactive version of the project's main software. This site has had tens of thousands of visits, with the interactive software in particular receiving nearly 200,000 hits. The project attracted much media attention, resulting in articles in the press, radio interviews, and a TV appearance.''')
             )]

_BAR = URIRef('http://bar')

def _foo(i):
    return URIRef('http://foo#{}'.format(i))

def _long_random_literal():
    return Literal(','.join([uuid4().hex for _ in range(200)]))
 
class TestCase(unittest.TestCase):
    def setUp(self):
        # First, create an instance of the Testbed class.
        self.testbed = testbed.Testbed()
        # Then activate the testbed, which prepares the service stubs for use.
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        self._sample_triples = set()

    def tearDown(self):
        self.testbed.deactivate()

    def testUTF8Literal(self):
        st = ndbstore.NDBStore(identifier = 'banana')
        st.add(_TRIPLES[0], None)
        self.assertEquals(1, len(st))
        self._assertSameSet(_TRIPLES[0:1], st.triples((None, None, None), None))
        
    def testALotOfData(self):
        st = ndbstore.NDBStore(identifier = 'alotofdata',
                               configuration = {'no_of_shards_per_predicate_dict': {_BAR: 16},})
        st.addN(self._manyLargeQuads())
        g = Graph(store = st)
        for triple in self._sample_triples:
            self.assertIn(triple, g, 'Did not find {} in store'.format(triple))
        
    def _manyLargeQuads(self):
        to_be_sampled = sample(xrange(1000), 10)
        for i in range(1000):
            quad = (_foo(i), _BAR, _long_random_literal(), None)
            if i in to_be_sampled:
                self._sample_triples.add(quad[0:3])
            yield quad
    
    def _assertSameSet(self, triple_list, quad_generator):
        self.assertEquals(set(triple_list), set([t for (t, _) in quad_generator]))
        
if __name__ == '__main__':
    unittest.main()
