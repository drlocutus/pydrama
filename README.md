pydrama
=======

This extension module provides a Python3 interface to AAO's DRAMA
communication framework, using Cython to wrap the C library.


Dependencies
------------

- `/local/python3` -- Python3 install containing `bin/`, `lib/` directories
- `/local/python3/bin/cython` -- >= v0.29.1
- `/local/python3/lib/python3.7/site-packages/numpy` -- >= v1.15.4
- `/local/python3/lib/python3.7/site-packages/jac_sw` -- from [pyuae]
- `/jac_sw/epics/CurrentRelease` -- EPICS R3.13.8 with UAE extensions
- `/jac_sw/drama/CurrentRelease` -- DRAMA v1.6.3
- `/jac_sw/itsroot/install/common` -- for Jit library

[pyuae]: https://github.com/eaobservatory/pyuae


Installation
------------

Create or copy a `config/CONFIG.Defs` file.  Example:

    APPLIC_BASE=/jac_sw/epics/CurrentRelease
    APPLIC_CONFIG=/jac_sw/epics/CurrentRelease/config
    APPLIC_DEPENDS=/jac_sw/drama/CurrentRelease /jac_sw/itsroot/install/common
    APPLIC_INSTALL=/jac_sw/itsroot/install
    APPLIC_SUBDIR_FILE=Makefile.Dirs
    APPLIC_TARGETS=
    APPLIC_TEMPLATES=/jac_sw/epics/CurrentRelease/templates/uae
    APPLIC_VERSION=pydrama_0p3_b64


Then

    $ make

This example installs to `/jac_sw/itsroot/install/pydrama_0p3_b64`.
For other scripts to be able to find `pydrama` using the `jac_sw` module,
you'll also need to create a softlink in the install directory:

    $ cd /jac_sw/itsroot/install
    $ ln -s pydrama_0p3_b64 pydrama


Documentation
-------------

HTML documentation can be generated using the scripts in the `doc/` directory:

    $ doc/pydoc.sh


Examples
--------

Example task scripts can be found in the `test/` directory.


Notes
-----

This module uses SDS only as a serialization method, and provides no
direct access to, or updates of, SDS structures held in memory.
Users deal directly with basic Python objects (dicts, lists, numpy arrays),
but some extra computation time and memory is needed when sending or
receiving messages.  Keep this in mind especially when publishing or monitoring
very large data structures.

The argument structure of a `get()` reply is slightly different from that of
a `monitor()` update.  For a `get()`, `msg.arg = {'PARAM':value}`, while on a
`MON_CHANGED` trigger, `msg.arg = value`.  Run the `get.py` and `mon.py`
examples with no arguments to see the difference on the `TIME` parameter.



