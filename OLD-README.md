rdflib-appengine
================

This aim of this project is to allow rdflib, the Python RDF library, to be deployed on Google App Engine (GAE) as a SPARQL endpoint. Data will be persisted in the NDB storage.

The project requires the following software to be installed:
  * https://developers.google.com/appengine/downloads#Google_App_Engine_SDK_for_Python
  * Python 2.7
  * pip (if you do not have it, try "easy_install pip"
  * make
  
Running "make" in the project root should download all required python packages, link them into the source appropriately and launch a local app server on port 3030.

Now try http://localhost:3030/ds/query?query=SELECT%20*%20WHERE%20{?x%20?r%20?y}

To update the data, you must POST a SPARQL update query to http://localhost:3030/ds/query

To try it out on Google App Engine:
  * Go to https://appengine.google.com
  * If you do not have an account, register
  * Create a new application
  * Change app.yaml to use your application's name (rdflib-ndb was taken by me, sorry)
  * Run "make deploy" locally and enter you account details
  
All of this can be done without allowing any kind of billing.
