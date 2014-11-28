MAJORMINOR := 1.0

SRCMAIN_FILES := $(shell find src/main -name "*.py")
NAME := $(shell grep name src/main/setup.py | cut -d "'" -f 2)
DISTFILE := dist/$(NAME)-$(MAJORMINOR).tar.gz
GAEDIR := build/rdflib-appengine-$(MAJORMINOR)

.PHONY: runlocal
runlocal: .gaebuild.made .tests.made
	dev_appserver.py $(GAEDIR) --port=3030 --log_level debug

.PHONY: gaebuild
gaebuild: .gaebuild.made

.PHONY: all
all: ide runlocal

.PHONY: test
test: .tests.made

.tests.made: src/test/testrunner.py src/test/suite/*.py .gaebuild.made
	src/test/testrunner.py $(shell dirname $(shell readlink $(shell which dev_appserver.py))) ./src/test/ $(GAEDIR) #TODO: This is not very portable
	touch .tests.made

.gaebuild.made: .gaebuild.example.made .gaebuild.srcmain.made
	touch .gaebuild.made

.gaebuild.srcmain.made: .gaedir.made $(DISTFILE)
	pip install -t $(GAEDIR) $(DISTFILE)
	touch .gaebuild.srcmain.made

.gaebuild.example.made: src/example/* .gaedir.made
	cp -r src/example $(GAEDIR)/
	touch .gaebuild.example.made

.gaedir.made:
	mkdir -p $(GAEDIR)
	touch .gaedir.made

.PHONY: ide
ide: .pip.for.ide.made

.PHONY: dist
dist: $(DISTFILE)

$(DISTFILE): $(SRCMAIN_FILES)
	mkdir -p dist
	(cd src/main/ && ./setup.py sdist --dist-dir ../../dist/)

.pip.for.ide.made: .venv.for.ide/bin/activate src/main/requirements.txt $(SRCMAIN_FILES)
	source .venv.for.ide/bin/activate && (cd src/main/ && pip install -r requirements.txt)
	touch .pip.for.ide.made

.venv.for.ide/bin/activate:
	virtualenv .venv.for.ide

.PHONY: clean
clean: distclean
	rm -rf .venv.*
	rm -rf build
    
.PHONY: distclean
distclean:
	rm .*.made
	rm -rf dist

    
