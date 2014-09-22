.PHONY: all
all: runlocal

.PHONY: runlocal
runlocal: rdflib/__init__.py
	dev_appserver.py --port=3030 .

.PHONY: runclean
runclean:
	dev_appserver.py . --port=3030 --clear_datastore true

rdflib/__init__.py:
	pip install --ignore-installed -t libs rdflib
	ln -s ./libs/rdflib ./rdflib
	ln -s ./libs/pkg_resources.py ./pkg_resources.py
	ln -s ./libs/six.py ./six.py
	ln -s ./libs/pyparsing.py ./pyparsing.py
	ln -s ./libs/isodate ./isodate
	ln -s ./libs/html5lib ./html5lib
	ln -s ./libs/SPARQLWrapper ./SPARQLWrapper

.PHONY: clean
clean:
	rm -f ./rdflib ./pkg_resources.py ./six.py ./pyparsing.py ./isodate ./html5lib ./SPARQLWrapper
	rm -rf libs/
	mkdir libs

.PHONY: deploy
deploy:
	appcfg.py update .
    

    
