fakeredis: A fake version of a redis-py
=======================================

[![badge](https://img.shields.io/pypi/v/fakeredis)](https://pypi.org/project/fakeredis/)
[![CI](https://github.com/cunla/fakeredis-py/actions/workflows/test.yml/badge.svg)](https://github.com/cunla/fakeredis-py/actions/workflows/test.yml)
[![badge](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/cunla/b756396efb895f0e34558c980f1ca0c7/raw/fakeredis-py.json)](https://github.com/cunla/fakeredis-py/actions/workflows/test.yml)
[![badge](https://img.shields.io/pypi/dm/fakeredis)](https://pypi.org/project/fakeredis/)
[![badge](https://img.shields.io/pypi/l/fakeredis)](./LICENSE)
[![Open Source Helpers](https://www.codetriage.com/cunla/fakeredis-py/badges/users.svg)](https://www.codetriage.com/cunla/fakeredis-py)
--------------------

Documentation is now hosted in https://fakeredis.readthedocs.io/

# Intro

fakeredis is a pure-Python implementation of the Redis key-value store.

This module is a reasonable substiture to running a redis-server. 
It allows running tests requiring redis server without an actual server.

It provides enhanced versions of the redis-py Python bindings for Redis. That provide the following added functionality:
* **Easy to use** - It provides a built in Redis server that is automatically installed, configured and managed when the Redis bindings are used.
* **Flexible** - Create a single server shared by multiple programs or multiple independent servers. All the servers provided by Redislite support all Redis  functionality including advanced features such as replication and clustering.
* **Compatible** - It provides enhanced versions of the Redis-Py python Redis bindings as well as functions to patch them to allow most existing code that uses them   to run with little or no modifications.
* **Secure** - It uses a secure default Redis configuraton that is only accessible by the creating user on the computer system it is run on.

# Sponsor

fakeredis-py is developed for free.

You can support this project by becoming a sponsor using [this link](https://github.com/sponsors/cunla).

## Security contact information

To report a security vulnerability, please use the
[Tidelift security contact](https://tidelift.com/security).
Tidelift will coordinate the fix and disclosure.
