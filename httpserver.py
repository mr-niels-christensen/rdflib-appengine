import webapp2
from rdflib import Graph
from google.appengine.api import memcache
import logging

class MainPage(webapp2.RequestHandler):
    def get(self):
        #Access-Control-Allow-Origin: *
        self.response.headers['Access-Control-Allow-Origin'] = '*'
        self.response.headers['Content-Type'] = 'application/sparql-results+json; charset=utf-8'
        self.response.write(query(self.request.get('query')))

    def post(self):
        update(self.request.body)
        self.response.set_status(201)

application = webapp2.WSGIApplication([
    ('/.*', MainPage),
], debug=True)

def update(q):
    g = get_data()
    g.update(q)
    set_data(g)
    
def query(q):
    return get_data().query(q).serialize(format='json')
    
def get_data():
    data = memcache.Client().get('key')
    if data is not None:
        logging.info("Hit")
        return data
    else:
        logging.info("Miss")
        g = Graph()
        memcache.Client().add('key', g)
        return g
    
def set_data(g):
    logging.info("Stored")
    memcache.Client().set('key', g)

