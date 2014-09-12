'''
Created on 12 Sep 2014

@author: s05nc4

This code borrows heavily from the Memory Store class by Michel Pelletier, Daniel Krech, Stefan Niederhauser
'''

from rdflib.store import Store

ANY = Any = None

class NDBStore(Store):
    """\
    A triple store using NDB on GAE (Google App Engine)
    """
    def __init__(self, configuration=None, identifier=None):
        super(NDBStore, self).__init__(configuration)
        if identifier is None or not isinstance(identifier, basestring):
            raise Exception("NDBStore requires a basestring identifier")
        self.identifier = identifier

    def addN(self, quads):
        for s, p, o, c in quads:
            assert c is None, \
                "Context associated with %s %s %s is not None but %s!" % (s, p, o, c)
        assert "Not implemented yet"

    def add(self, (subject, predicate, o), context, quoted=False):
        """\
        Redirects to addN() because NDB heavily favours batch updates.
        """
        self.addN([(subject, predicate, o, context)])

    def remove(self, (subject, predicate, o), context=None):
        assert "Not implemented yet"

    def triples(self, (subject, predicate, o), context=None):
        """A generator over all the triples matching """
        assert "Not implemented yet"

    def __len__(self, context=None):
        assert "Not implemented yet"

    def bind(self, prefix, namespace):
        assert "Not implemented yet"

    def namespace(self, prefix):
        assert "Not implemented yet"

    def prefix(self, namespace):
        assert "Not implemented yet"

    def namespaces(self):
        assert "Not implemented yet"

    def __contexts(self):
        return (c for c in [])  # TODO: best way to return empty generator
