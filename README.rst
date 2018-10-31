Guillotina Couchbase Docs
=========================

Integration of couchbase with Guillotina.

TODO:

- [x] basic storage implementation
- [ ] catalog
- [ ] blob support


Dependencies
------------

Python >= 3.6


Installation
------------

This example will use virtualenv::

  brew update
  brew install libcouchbase
  python -m venv .
  ./bin/pip install -e .


Running
-------

Most simple way to get running::

  ./bin/guillotina


Running couchbase with docker
-----------------------------

docker run -p 8091-8094:8091-8094 -p 11210:11210 couchbase