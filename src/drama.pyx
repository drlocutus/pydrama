#encoding: utf8
#cython: embedsignature=True
#cython: c_string_type=str
#cython: c_string_encoding=default
'''
drama.pyx   RMB 20140926

DRAMA Python module.

A note on logging/debug output:

    This module uses the standard python 'logging' module, not jitDebug.
    The logger name is "drama".
    A NullHandler is installed to prevent Cython exceptions.
    The log level is set to INFO.  If you require debug output, call:
    
        logging.getLogger('drama').setLevel(logging.DEBUG)

    For a basic logging config that ignores all the debug output
    from this module, logs to a file in /jac_logs/YYYYMMDD,
    and sends INFO to MsgOut and >=WARN to ErsOut, try:

        import drama.log
        drama.log.setup('MYTASKNAME')

    Note that logging calls explicitly list their function context
    instead of relying on on '%(funcName)s' in the handler format;
    this is due to an apparent bug in cython extension modules
    that causes '%(funcName)s' to produce '<module>' instead of
    something useful.  Users are advised to do the same in their own code
    instead of using the '%(funcName)s' format.

    Note also that this module expects a MsgOutHandler to be installed
    so that any errors will be properly sent to the calling task;
    the module never makes any explicit MsgOut/ErsOut/ErsRep calls of its own.


Copyright (C) 2020 East Asian Observatory

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
'''


from drama cimport *

import sys as _sys
import time as _time
import select as _select
import errno as _errno
import numpy as _numpy
cimport numpy as _numpy
import logging as _logging


# populate error message tables
jitPutFacilities()


############### Global Task Vars #########################


# Replicate jit default_info for our own Path ops
cdef DitsPathInfoType _default_path_info

# tideInit() copies the pointer to this local, so it has to stay alive.
cdef DitsAltInType _altin = NULL

# The main DRAMA file descriptor from DitsGetXInfo().
cdef int _fd = -1

# Save task name for stop() and such
_taskname = ""

# Stack to track current action name (last index)
_action_stack = []

# Number of times each action appears in _action_stack (dispatcher depth)
_activity = {}

# Reschedule status (True/False) for each action
_rescheduled = {}

# Outstanding monitors, {action:[(task,monid),...]}
_monitors = {}

# Registered task actions, {name:func}.
_actions = {}

# Registered callbacks, {fd:func}.
_callbacks = {}

# Logging config is left for the user
#_log = _logging.getLogger(__name__)  # drama.__drama__, not great
_log = _logging.getLogger('drama')
_log.addHandler(_logging.NullHandler())  # avoid 'no handlers' exception
_log.setLevel(_logging.INFO)  # be quiet even if root level is DEBUG

# Global exit flag, set by Exit.__init__, to break loops
_g_exit_flag = 0


############ Global Lookup Dictionaries #################


_sds_code_string = {
    SDS_STRUCT: 'SDS_STRUCT',
    SDS_CHAR:   'SDS_CHAR',
    SDS_UBYTE:  'SDS_UBYTE',
    SDS_BYTE:   'SDS_BYTE',
    SDS_USHORT: 'SDS_USHORT',
    SDS_SHORT:  'SDS_SHORT',
    SDS_UINT:   'SDS_UINT',
    SDS_INT:    'SDS_INT',
    SDS_FLOAT:  'SDS_FLOAT',
    SDS_DOUBLE: 'SDS_DOUBLE',
    SDS_I64:    'SDS_I64',
    SDS_UI64:   'SDS_UI64'
}

_sds_code_to_dtype = {
    SDS_CHAR:    _numpy.int8,
    SDS_UBYTE:   _numpy.uint8,
    SDS_BYTE:    _numpy.int8,
    SDS_USHORT:  _numpy.uint16,
    SDS_SHORT:   _numpy.int16,
    SDS_UINT:    _numpy.uint32,
    SDS_INT:     _numpy.int32,
    SDS_FLOAT:   _numpy.float32,
    SDS_DOUBLE:  _numpy.float64,
    SDS_I64:     _numpy.int64,
    SDS_UI64:    _numpy.uint64
}

# use with str(array.dtype)
_dtype_to_sds_code = {
    'bool':    SDS_BYTE,
    'float32': SDS_FLOAT,
    'float64': SDS_DOUBLE,
    'int8':    SDS_BYTE,
    'int16':   SDS_SHORT,
    'int32':   SDS_INT,
    'int64':   SDS_I64,
    'uint8':   SDS_UBYTE,
    'uint16':  SDS_USHORT,
    'uint32':  SDS_UINT,
    'uint64':  SDS_UI64
}

# use with array.dtype.type
_ntype_to_sds_code = {
    _numpy.bool_:   SDS_BYTE,
    _numpy.float32: SDS_FLOAT,
    _numpy.float64: SDS_DOUBLE,
    _numpy.int8:    SDS_BYTE,
    _numpy.int16:   SDS_SHORT,
    _numpy.int32:   SDS_INT,
    _numpy.int64:   SDS_I64,
    _numpy.uint8:   SDS_UBYTE,
    _numpy.uint16:  SDS_USHORT,
    _numpy.uint32:  SDS_UINT,
    _numpy.uint64:  SDS_UI64,
}

# use with array.dtype.str
#_dtype_to_sds_code = {
#    '|b1': SDS_BYTE,
#    '<f4': SDS_FLOAT,
#    '<f8': SDS_DOUBLE,
#    '|i1': SDS_BYTE,
#    '<i2': SDS_SHORT,
#    '<i4': SDS_INT,
#    '<i8': SDS_I64,
#    '|u1': SDS_UBYTE,
#    '<u2': SDS_USHORT,
#    '<u4': SDS_UINT,
#    '<u8': SDS_UI64
#}

# expose constants to python
REA_OBEY             = DITS_REA_OBEY
REA_KICK             = DITS_REA_KICK
REA_RESCHED          = DITS_REA_RESCHED
REA_TRIGGER          = DITS_REA_TRIGGER
REA_ASTINT           = DITS_REA_ASTINT
REA_LOAD             = DITS_REA_LOAD
REA_LOADFAILED       = DITS_REA_LOADFAILED
REA_MESREJECTED      = DITS_REA_MESREJECTED
REA_COMPLETE         = DITS_REA_COMPLETE
REA_DIED             = DITS_REA_DIED
REA_PATHFOUND        = DITS_REA_PATHFOUND
REA_PATHFAILED       = DITS_REA_PATHFAILED
REA_MESSAGE          = DITS_REA_MESSAGE
REA_ERROR            = DITS_REA_ERROR
REA_EXIT             = DITS_REA_EXIT
REA_NOTIFY           = DITS_REA_NOTIFY
REA_BULK_TRANSFERRED = DITS_REA_BULK_TRANSFERRED
REA_BULK_DONE        = DITS_REA_BULK_DONE

APP_ERROR     = DITS__APP_ERROR
APP_TIMEOUT   = DITS__APP_TIMEOUT
MON_STARTED   = DITS__MON_STARTED
MON_CHANGED   = DITS__MON_CHANGED
NOTUSERACT    = DITS__NOTUSERACT
INVARG        = DITS__INVARG
INVPATH       = DITS__INVPATH
UNEXPMSG      = DITS__UNEXPMSG
EXITHANDLER   = DITS__EXITHANDLER

# TODO expose this
_entry_reason_string = {
    DITS_REA_OBEY:             "DITS_REA_OBEY",
    DITS_REA_KICK:             "DITS_REA_KICK",
    DITS_REA_RESCHED:          "DITS_REA_RESCHED",
    DITS_REA_TRIGGER:          "DITS_REA_TRIGGER",
    DITS_REA_ASTINT:           "DITS_REA_ASTINT",
    DITS_REA_LOAD:             "DITS_REA_LOAD",
    DITS_REA_LOADFAILED:       "DITS_REA_LOADFAILED",
    DITS_REA_MESREJECTED:      "DITS_REA_MESREJECTED",
    DITS_REA_COMPLETE:         "DITS_REA_COMPLETE",
    DITS_REA_DIED:             "DITS_REA_DIED",
    DITS_REA_PATHFOUND:        "DITS_REA_PATHFOUND",
    DITS_REA_PATHFAILED:       "DITS_REA_PATHFAILED",
    DITS_REA_MESSAGE:          "DITS_REA_MESSAGE",
    DITS_REA_ERROR:            "DITS_REA_ERROR",
    DITS_REA_EXIT:             "DITS_REA_EXIT",
    DITS_REA_NOTIFY:           "DITS_REA_NOTIFY",
    DITS_REA_BULK_TRANSFERRED: "DITS_REA_BULK_TRANSFERRED",
    DITS_REA_BULK_DONE:        "DITS_REA_BULK_DONE"
}


############### Functions and Classes #########################


def segfault():
    '''Test function: trigger a segfault.'''
    cdef int *ptr = NULL
    return ptr[0]


def errors_from_header(filename):
    '''
    Look for error code definitions from a header file and return them
    as a dictionary; use when raising a BadStatus exception.
    This is intended as a convenience function for other tasks
    and modules; it is not used by this module directly.

    TODO: raise an error on empty dict?
    '''
    d = {}
    with open(filename) as f:
        for line in f:
            try:
                toks = line.split()
                assert len(toks) == 3
                assert toks[0] == '#define'
                d[toks[1]] = int(toks[2], 0)  # base 0 = guess
            except:
                pass
    return d


def get_status_string(status):
    '''Return the message string for a numeric status code.'''
    cdef char buf[256]
    MessGetMsg(status, -1, sizeof(buf), buf)
    return str(buf).replace('%', '')


class DramaException(Exception):
    '''Common base class for DITS exceptions.'''
    pass


class Exit(DramaException):
    '''
    Raise to cause task to exit.  Calls blind_obey(_taskname, "EXIT")
    during __init__ to ensure the task dies even the exception is caught.
    '''
    def __init__(self, *arg, **kwarg):
        global _g_exit_flag
        super(Exit, self).__init__(*arg, **kwarg)
        _g_exit_flag = 1
        blind_obey(_taskname, "EXIT")
        _log.info('Exit(%s, %s) sent %s EXIT', arg, kwarg, _taskname)


class BadStatus(DramaException):
    '''
    Raised when internal functions return with bad status.
    Saves status (first argument) and status_string (from get_status_string);
    message (second argument) should be the function or reason
    for the bad status value.

    Example:
        jitAppInit(taskname, &status)
        if status != 0:
            raise BadStatus(status, 'jitAppInit("%s")' % (taskname))
    '''

    def __init__(self, status, message):
        '''Create BadStatus with numeric status and descriptive message.'''
        self.status = status
        self.status_string = get_status_string(status)
        self.message = message
        self.args = (self.message, self.status, self.status_string)


def delete_sds(id):
    '''SdsDelete and SdsFreeId id, ignoring errors.'''
    cdef StatusType status = 0
    SdsDelete(id, &status)
    SdsFreeId(id, &status)


def sds_info(id):
    '''Given SDS id, return name, typecode, dims (in original dits order).'''
    cdef StatusType status = 0
    cdef char name[80]
    cdef SdsCodeType code
    cdef long ndims
    cdef ulong cdims[7]
    SdsInfo(id, name, &code, &ndims, cdims, &status)
    if status != 0:
        raise BadStatus(status, "SdsInfo(%d)" % (id))
    dims = None
    if ndims > 0:
        dims = [0] * ndims
        for i in xrange(ndims):
            dims[i] = cdims[i]
    return str(name), int(code), dims


def sds_from_obj(obj, name="", pid=0):
    '''
    Given python object, recursively construct and return a new SDS id
    with optional name and parent SDS id.
    This function is the python wrapper for c-only _sds_from_obj().
    '''
    return _sds_from_obj(obj, name, pid)


cdef SdsIdType _sds_from_obj(object obj, char* name="", SdsIdType pid=0):
    '''
    Given python object, recursively construct and return a new SDS id
    with optional name and parent SDS id.
    This is the c-only function for faster recursive calls.
    '''
    cdef SdsIdType id = 0
    cdef StatusType status = 0
    cdef ulong cdims[7]
    cdef ulong cindex[7]
    # the below are for simple scalars
    cdef char ci8
    cdef short ci16
    cdef int ci32
    cdef long long ci64
    cdef unsigned char cu8
    cdef unsigned short cu16
    cdef unsigned int cu32
    cdef unsigned long long cu64
    cdef float cfloat
    cdef double cdouble
    cdef void* cdata
    cdef SdsCodeType ccode
    cdef ulong cbytes

    if obj is None:
        # create an undefined placeholder.  TODO: this doesn't work.
        SdsNew(pid, name, 0, NULL, SDS_INT, 0, NULL, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,SDS_INT)" % (pid,name))
        return id

    if isinstance(obj, dict):
        SdsNew(pid, name, 0, NULL, SDS_STRUCT, 0, NULL, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,SDS_STRUCT)" % (pid,name))
        for k in obj.keys():
            kid = _sds_from_obj(obj[k], k, id)
            SdsFreeId(kid, &status)
        return id
    elif isinstance(obj, (bytes,str)):  # worth optimizing
        if not isinstance(obj, bytes):
            obj = obj.encode()
        n = obj.find(b'\0')
        if n >= 0:
            obj = obj[:n+1]
        else:
            obj = obj + b'\0'
        cdims[0] = len(obj)
        SdsNew(pid, name, 0, NULL, SDS_CHAR, 1, cdims, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,SDS_CHAR,[%d])" % (pid, name, len(obj)))
        SdsPut(id, len(obj), 0, <char*>obj, &status)
        if status != 0:
            # NOTE obj could be huge, so first 16 chars only
            dots = ''
            if len(obj) > 16:
                dots = '...'
            raise BadStatus(status, "SdsPut(%d,%d,0,%s%s)" % (id, len(obj), obj[:16], dots))
        return id
    elif isinstance(obj, _numpy.number):
        code = _ntype_to_sds_code[obj.dtype.type]
        SdsNew(pid, name, 0, NULL, code, 0, cdims, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,%s)" % (pid, name, _sds_code_string[code]))
        #SdsPut(id, obj.nbytes, 0, <void*>obj.data, &status)  # wrong
        #SdsPut(id, obj.nbytes, 0, (<_numpy.ndarray>obj).data, &status)  # segfault
        #SdsPut(id, obj.nbytes, 0, <void*>&obj[()], &status)  # can't take address of object
        # lame, but i can't seem to get a data pointer from a numpy.number
        if code == SDS_BYTE:
            ci8 = obj
            cdata = <void*>&ci8
        elif code == SDS_SHORT:
            ci16 = obj
            cdata = <void*>&ci16
        elif code == SDS_INT:
            ci32 = obj
            cdata = <void*>&ci32
        elif code == SDS_I64:
            ci64 = obj
            cdata = <void*>&ci64
        elif code == SDS_UBYTE:
            cu8 = obj
            cdata = <void*>&cu8
        elif code == SDS_USHORT:
            cu16 = obj
            cdata = <void*>&cu16
        elif code == SDS_UINT:
            cu32 = obj
            cdata = <void*>&cu32
        elif code == SDS_UI64:
            cu64 = obj
            cdata = <void*>&cu64
        elif code == SDS_FLOAT:
            cfloat = obj
            cdata = <void*>&cfloat
        else:
            cdouble = obj
            cdata = <void*>&cdouble
        SdsPut(id, obj.nbytes, 0, cdata, &status)
        if status != 0:
            raise BadStatus(status, "SdsPut(%d,%d,0,%s)" % (id, obj.nbytes, obj.tobytes()))
        return id
    elif isinstance(obj, bool):
        ci8 = obj
        SdsNew(pid, name, 0, NULL, SDS_BYTE, 0, cdims, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,SDS_BYTE)" % (pid, name))
        SdsPut(id, 1, 0, <void*>&ci8, &status)
        if status != 0:
            raise BadStatus(status, "SdsPut(%d,1,0,%s)" % (id, obj))
        return id
    elif isinstance(obj, float):
        cdouble = obj
        SdsNew(pid, name, 0, NULL, SDS_DOUBLE, 0, cdims, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,SDS_DOUBLE)" % (pid, name))
        SdsPut(id, 8, 0, <void*>&cdouble, &status)
        if status != 0:
            raise BadStatus(status, "SdsPut(%d,8,0,%s)" % (id, obj))
        return id
    elif isinstance(obj, int):
        if obj < -2147483648:
            ci64 = obj
            cdata = <void*>&ci64
            ccode = SDS_I64
            cbytes = 8
        elif obj < -32768:
            ci32 = obj
            cdata = <void*>&ci32
            ccode = SDS_INT
            cbytes = 4
        elif obj < -128:
            ci16 = obj
            cdata = <void*>&ci16
            ccode = SDS_SHORT
            cbytes = 2
        elif obj < 0:
            ci8 = obj
            cdata = <void*>&ci8
            ccode = SDS_BYTE
            cbytes = 1
        elif obj < 256:
            cu8 = obj
            cdata = <void*>&cu8
            ccode = SDS_UBYTE
            cbytes = 1
        elif obj < 65536:
            cu16 = obj
            cdata = <void*>&cu16
            ccode = SDS_USHORT
            cbytes = 2
        elif obj < 4294967296:
            cu32 = obj
            cdata = <void*>&cu32
            ccode = SDS_UINT
            cbytes = 4
        else:
            cu64 = obj
            cdata = <void*>&cu64
            ccode = SDS_UI64
            cbytes = 8
        SdsNew(pid, name, 0, NULL, ccode, 0, cdims, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,%s)" % (pid, name, _sds_code_string[ccode]))
        SdsPut(id, cbytes, 0, cdata, &status)
        if status != 0:
            raise BadStatus(status, "SdsPut(%d,%d,0,%s)" % (id, cbytes, obj))
        return id
    else:
        # cast whatever it is to a numpy array, query dtype.
        # this can end up casting everything to strings :/
        #obj = _numpy.array(obj)
        obj = _numpy.asanyarray(obj)  # no copy if obj is already an ndarray
        dtype = str(obj.dtype)
        #dtype = obj.dtype.str  # no real speed improvement
        shape = obj.shape

    # unicode (py3 str) gets type <U, 4 bytes per char with nulls.
    # convert to byte strings using preferred encoding
    if dtype.startswith("<U") or dtype.startswith(">U"):
        #obj = _numpy.array(obj, dtype='|S')  # always uses ascii, even in py3
        obj = _numpy.char.encode(obj)  # uses str.encode
        dtype = str(obj.dtype)
    
    # for strings, append strlen to dims; get non-struct typecode
    if dtype.startswith("|S"):
        # some DRAMA ops expect null-terminated strings, but
        # python strings usually aren't.
        #slen = int(dtype[2:])
        slen = obj.dtype.itemsize
        #maxlen = _numpy.max([len(x) for x in obj.flat])  # slow!
        maxlen = max([len(x) for x in obj.flat])
        if maxlen == slen:  # no space for \0
            slen += 1
            #dtype = '|S%d' % (slen)
            dtype = '|S' + str(slen)
            obj = _numpy.array(obj, dtype=dtype)
        shape = list(shape)
        shape.append(slen)
        code = SDS_CHAR
    elif dtype != 'object':
    #elif not dtype.startswith("|O"):
        code = _dtype_to_sds_code[dtype]

    # reverse numpy dim order for dits
    for i in xrange(len(shape)):
        cdims[i] = shape[-(1+i)]

    if dtype == 'object':
    #if dtype.startswith("|O"):
        # make sure every item is a dict
        for index in _numpy.ndindex(shape):
            if not isinstance(obj[index], dict):
                raise TypeError("invalid array type " + str(type(obj[index])))
        # create struct array and fill it in
        SdsNew(pid, name, 0, NULL, SDS_STRUCT, len(shape), cdims, &id, &status)
        if status != 0:
            raise BadStatus(status, "SdsNew(%d,%s,SDS_STRUCT,%s)" % \
                            (pid, name, list(reversed(shape))) )
        for index in _numpy.ndindex(shape):
            kid = _sds_from_obj(obj[index], name, 0)
            # reverse numpy index order for dits and make 1-based
            for i in xrange(len(index)):
                cindex[i] = index[-(1+i)] + 1
            SdsInsertCell(id, len(index), cindex, kid, &status)
            if status != 0:
                raise BadStatus(status, "SdsInsertCell(%d,%s,%d)" % \
                                (id, list(reversed(index)), kid) )
            SdsFreeId(kid, &status)
        return id

    SdsNew(pid, name, 0, NULL, code, len(shape), cdims, &id, &status)
    if status != 0:
        raise BadStatus(status, "SdsNew(%d,%s,%s,%s)" % (pid, name,
                        _sds_code_string[code], list(reversed(shape))) )
    #obuf = obj.tostring()
    #SdsPut(id, obj.nbytes, 0, <char*>obuf, &status)
    # cast to ctype ndarray to avoid expensive tostring() conversion
    SdsPut(id, obj.nbytes, 0, (<_numpy.ndarray>obj).data, &status)
    #SdsPut(id, obj.nbytes, 0, <void*>obj.data, &status)  # wrong, segfault
    if status != 0:
        # NOTE obuf could be huge, so first 16 chars only :/
        dots = ''
        if len(obj.data) > 16:
            dots = '...'
        raise BadStatus(status, "SdsPut(%d,%d,0,%s%s)" % \
                        (id, obj.nbytes, obj.data[:16], dots) )
    return id


def obj_from_sds(id):
    '''
    Given an SDS id, recursively construct and return a python object.
    This is the python wrapper for the c-only _obj_from_sds().
    '''
    return _obj_from_sds(id)


cdef object _obj_from_sds(SdsIdType id):
    '''
    Given an SDS id, recursively construct and return a python object.
    This is the c-only function for faster recursive calls.
    '''
    cdef StatusType status = 0
    cdef SdsIdType cid
    cdef ulong cindex[7]
    cdef void* buf
    cdef ulong buflen

    if id == 0:
        return None
    
    # RMB 20190206: Removed the _log.debug calls.
    # Even if NOP (level>DEBUG), they take up too much time here.

#    _log.debug('_obj_from_sds: calling sds_info(%d)', id)
    name, code, dims = sds_info(id)
#    _log.debug('_obj_from_sds: name,code,dims: %s, %s, %s', name, code, dims)

    # dits dim/index ordering is reversed vs numpy.
    if dims is not None:
        dims.reverse()

    # NOTE: Given numpy scalar weirdness and the desire not to
    # deal with shapeless array access (ie, a[()] = 3.14),
    # scalars become basic python values and hence SDS objects
    # will not 'round-trip'.  I think in practice this will
    # not be a problem since most conversions will be one-way only.

    if code == SDS_STRUCT:
        if dims is None:
            # single struct, loop over members to fill out dict
            obj = {}
            i = 1  # DRAMA is 1-based...WHY
            while status == 0:
#                _log.debug('_obj_from_sds: calling SdsIndex(%d)', i)
                SdsIndex(id, i, &cid, &status)
                if status != 0:
                    break
#                _log.debug('_obj_from_sds: calling sds_info(%d)', cid)
                cname, dummy, dummy = sds_info(cid)
#                _log.debug('_obj_from_sds: recursing on %s', cname)
                obj[cname] = _obj_from_sds(cid)
                i += 1
#                _log.debug('_obj_from_sds: SdsFreeId(%d)', cid)
                SdsFreeId(cid, &status)
#            _log.debug('_obj_from_sds: returning %s', obj)
            return obj
        else:
            # create numpy array of python objs and fill it out
            obj = _numpy.ndarray(shape=dims, dtype=object)
            for index in _numpy.ndindex(obj.shape):
                for i in xrange(len(index)):
                    cindex[i] = index[-(1+i)] + 1  # again, reversed, 1-based
                SdsCell(id, len(index), cindex, &cid, &status)
                if status != 0:
                    raise BadStatus(status, "SdsCell(%d,%s)" % \
                                    (id, list(reversed(index))) )
                obj[index] = _obj_from_sds(cid)
                SdsFreeId(cid, &status)
            return obj

    # for anything else we need the raw buffer
#    _log.debug('_obj_from_sds: SdsPointer(%d)', id)
    SdsPointer(id, &buf, &buflen, &status)
    if status == SDS__UNDEFINED:
        return None
    if status != 0:
        raise BadStatus(status, "SdsPointer(%d)" % (id))
    
    # skip bytearray/ndarray conversion for simple types.
    # NOTE we do not cast scalars to numpy types, since any operations
    # using those values will upcast (bloat) to int64 or float64.
    if dims is None:
        if code == SDS_BYTE:
            return (<char*>buf)[0]
        elif code == SDS_UBYTE:
            return (<unsigned char*>buf)[0]
        elif code == SDS_SHORT:
            return (<short*>buf)[0]
        elif code == SDS_USHORT:
            return (<unsigned short*>buf)[0]
        elif code == SDS_INT:
            return (<int*>buf)[0]
        elif code == SDS_UINT:
            return (<unsigned int*>buf)[0]
        elif code == SDS_I64:
            return (<long long*>buf)[0]
        elif code == SDS_UI64:
            return (<unsigned long long*>buf)[0]
        elif code == SDS_FLOAT:
            return (<float*>buf)[0]
        elif code == SDS_DOUBLE:
            return (<double*>buf)[0]
    
    #_log.debug('_obj_from_sds: calling PyBytes_FromStringAndSize(%x, %d)', <unsigned long>buf, buflen)
    #sbuf = PyBytes_FromStringAndSize(<char*>buf, buflen)
    # use a (mutable) bytearray so ndarray is WRITEABLE without needing to .copy().
    sbuf = PyByteArray_FromStringAndSize(<char*>buf, buflen)
    #_log.debug('_obj_from_sds: sbuf %s: %r', type(sbuf), sbuf)

    # issue: UNDEFINED stuff doesn't seem to work.  "external" flag?
    
    if code == SDS_CHAR:
#        _log.debug('_obj_from_sds: SDS_CHAR')
        if dims is None or len(dims) < 2:
            n = sbuf.find(b'\0')
            if n >= 0:
                sbuf = sbuf[:n]
            try:
                sbuf = str(sbuf.decode())
            except UnicodeDecodeError:
                sbuf = str(sbuf.decode('latin-1'))
#            _log.debug('_obj_from_sds: return sbuf %s', sbuf)
            return sbuf
        dtype = '|S%d' % (dims[-1])
        obj = _numpy.ndarray(shape=dims[:-1], dtype=dtype, buffer=sbuf)#.copy()
        # clean up the strings so they look nicer when printed;
        # trailing garbage will show up if non-null.
        for index in _numpy.ndindex(obj.shape):
            n = obj[index].find(b'\0')
            if n >= 0:
                obj[index] = obj[index][:n]
        # for python3, convert to str
        if _sys.version[0] == '3':
#            obj = obj.astype(object)
#            for i,x in _numpy.ndenumerate(obj):
#                try:
#                    obj[i] = x.decode()
#                except UnicodeDecodeError:
#                    obj[i] = x.decode('latin-1')
#            obj = obj.astype(str)
            try:
                obj = _numpy.char.decode(obj)
            except UnicodeDecodeError:
                obj = _numpy.char.decode(obj, 'latin-1')
#        _log.debug('_obj_from_sds: return obj %s', obj)
        return obj

#    _log.debug('_obj_from_sds: ndarray')
    dtype = _sds_code_to_dtype[code]
#    _log.debug('_obj_from_sds: ndarray(%s, %s, %s)', dims, dtype, sbuf)
    obj = _numpy.ndarray(shape=dims, dtype=dtype, buffer=sbuf)#.copy()
#    _log.debug('_obj_from_sds: past ndarray, obj %r', obj)
    #_log.debug('_obj_from_sds: trying [()]')
    #obj = obj[()]  # this will deref a scalar array or return original array.
    #_log.debug('_obj_from_sds: past [()]')
    if not obj.shape:
        obj = obj.dtype.type(obj)
#    _log.debug('_obj_from_sds: returning %s', obj)
    return obj


def sds_from_xml(buf):
    '''Return a new SDS structure id from XML buf (data or filename).'''
    cdef SdsIdType id = 0
    cdef StatusType status = 0
    # if buf is unicode, possibly len(buf) != len(bytes(buf)).  convert first.
    cdef char * cbuf = buf
    jitXML2Sds(strlen(cbuf), cbuf, &id, &status)
    if status != 0:
        raise BadStatus(status, "jitXML2Sds(%s)" % (buf))
    return id


def obj_from_xml(buf):
    '''Return python object parsed from XML buf (data or filename).'''
    id = sds_from_xml(buf)
    name, code, dims = sds_info(id)
    obj = obj_from_sds(id)
    delete_sds(id)
    return {name:obj}


def make_argument(*args, **kwargs):
    '''
    Return a new SDS argument structure from given args/kwargs;
    positional args are inserted into the structure
    as fields named 'Argument<n>'.
    '''
    for i,v in enumerate(args):
        kwargs['Argument%d' % (i+1)] = v
    argid = sds_from_obj(kwargs, 'ArgStructure')
    return argid


def parse_argument(arg):
    '''
    Given dict, return positional arg list and kw arg dict (args, kwargs).
    Input fields named 'Argument<n>' are pulled out to
    create the positional arg list.
    
    NOTE: This function can return objects with references to those in the
    given arg, or the given arg itself.  Modifying the returned objects could
    therefore modify the original arg -- be careful.
    '''
    if arg is None:
        return [],{}
    elif isinstance(arg, list):
        return arg,{}
    elif not isinstance(arg, dict):
        return [arg],{}
    kwargs = {}
    pargs = {}
    for k in arg.keys():
        if len(k) > 8 and k.startswith('Argument'):
            try:
                pargs[int(k[8:])] = arg[k]
            except ValueError:
                kwargs[k] = arg[k]
        else:
            kwargs[k] = arg[k]
    pargs = [pargs[x] for x in sorted(pargs.keys())]
    return pargs,kwargs


def get_param(name):
    '''
    Get a named value from the task's SDP parameter system.

    NOTE: This only works correctly for top-level params;
          trying to get MYSTRUCT.MYFIELD results in 'No such item'.
          TODO: Check name?
    '''
    cdef StatusType status = 0
    cdef SdsIdType item = 0
    cdef SdsIdType parid = <SdsIdType>DitsGetParId()
    SdsFind(parid, name, &item, &status)
    if status != 0:
        raise BadStatus(status, "SdsFind(%d,%s)" % (parid, name) )
    value = obj_from_sds(item)
    SdsFreeId(item, &status)
    return value


def set_param(name, value, drama=True, tide=False):
    '''
    Set a named value in the task's SDP parameter system.
    If drama=True, calls SdpUpdate() to update monitors.
    If tide=True, calls tideSetParam to update EPICS.

    NOTE: This only works correctly for top-level params;
          trying to set MYSTRUCT.MYFIELD will screw up.
          TODO: Check name and enforce.
    '''
    cdef StatusType status = 0
    cdef SdsIdType old = 0
    cdef SdsIdType parid = <SdsIdType>DitsGetParId()
    cdef DitsPathType epics_path = NULL
    newid = sds_from_obj(value, name)
    try:
        SdsFind(parid, name, &old, &status)
        if status == 0:
            delete_sds(old)
        status = 0
        SdsInsert(parid, newid, &status)
        if status != 0:
            raise BadStatus(status, "SdsInsert(%d,%d)" % (parid, newid) )
        if drama:
            SdpUpdate(newid, &status)
            if status != 0:
                raise BadStatus(status, "SdpUpdate(%d)" % (newid))
        if tide:
            tidePathGet("EPICS", NULL, 0, NULL, &epics_path, NULL, &status)
            if status != 0:
                raise BadStatus(status, "tidePathGet()")
            tideSetParam(epics_path, name, newid, NULL, &status);
            if status != 0:
                raise BadStatus(status, "tideSetParam(%s,%d)" % (name, newid))
    finally:
        SdsFreeId(newid, &status)


class Message:
    '''
    Message object holds entry parameters and action arguments.
    Instance attributes:
        time      float, time.time() of Message() creation
        entry     str, entry name from DitsGetEntInfo()
        task      str, task name from DitsGetEntInfo() or DitsGetParentPath()
        name      str, action name from DitsGetName() -- TODO call 'action'?
        transid   int, transaction id (address) from DitsGetEntInfo()
        reason    int, entry reason from DitsGetEntInfo()
        status    int, entry status from DitsGetEntInfo()
        arg_name  str, arg name from SdsInfo(DitsGetArgument())
        arg       ???, arg value
        arg_extra str, arg string from SdsGetExtra(DitsGetArgument())
    '''

    def __init__(self):
        '''Constructs a Message object from Dits entry parameters.'''
        cdef StatusType status = 0
        cdef char ent_name[DITS_C_NAMELEN]
        cdef char act_name[DITS_C_NAMELEN]
        cdef char ent_task[DITS_C_NAMELEN]
        cdef char extra[DITS_C_NAMELEN]
        cdef ulong extra_len = 0
        cdef DitsPathType ent_path
        cdef DitsTransIdType ent_transid
        cdef DitsReasonType ent_reason
        cdef StatusType ent_status
        
        _log.debug('Message(): calling DitsGetEntInfo')
        DitsGetEntInfo (DITS_C_NAMELEN, ent_name, &ent_path, &ent_transid,
                        &ent_reason, &ent_status, &status)
        if status != 0:
            raise BadStatus(status, "DitsGetEntInfo")

        # entry name is not necessarily the action name.
        _log.debug('Message(): calling DitsGetName')
        DitsGetName(DITS_C_NAMELEN, act_name, &status)
        # but this might be the orphan handler, no name
        if status == DITS__NOTUSERACT:
            status = 0
            strcpy(act_name, "_ORPHAN_")
        if status != 0:
            raise BadStatus(status, "DitsGetName")

        # get the calling task from the entry path.
        # fall back to parent path if entry path is NULL.
        if <ulong>ent_path == 0:
            _log.debug('Message(): calling DitsGetParentPath')
            ent_path = DitsGetParentPath()
        # get taskname if non-NULL path, otherwise shrug and give up
        if <ulong>ent_path != 0:
            _log.debug('Message(): calling DitsTaskFromPath')
            DitsTaskFromPath(ent_path, DITS_C_NAMELEN, ent_task, &status)
            if status != 0:
                raise BadStatus(status,
                                "DitsTaskFromPath(0x%x)" % (<ulong>ent_path) )
        else:
            # TODO maybe get path from ent_transid?  has a field for it.
            # would have to pull in and cast as Dits___TransIdType* tho.
            strcpy(ent_task, "???")

        # get message argument, separate positional/keyword parameters
        _log.debug('Message(): calling DitsGetArgument')
        argid = DitsGetArgument()
        _log.debug('Message(): calling obj_from_sds(%d)', argid)
        self.arg = obj_from_sds(argid)

        self.arg_name = None
        self.arg_extra = None
        if argid != 0:
            _log.debug('Message(): calling sds_info(%d)', argid)
            self.arg_name, arg_code, arg_dims = sds_info(argid)
            _log.debug('Message(): calling SdsGetExtra')
            SdsGetExtra(argid, DITS_C_NAMELEN, extra, &extra_len, &status)
            if status != 0:
                raise BadStatus(status, "SdsGetExtra(%d)" % (argid))
            if extra_len != 0:
                self.arg_extra = str(extra)

        # fill in the Message instance
        self.time = _time.time()
        self.entry = str(ent_name)
        self.task = str(ent_task)
        self.name = str(act_name)
        self.transid = int(<ulong>ent_transid)
        self.reason = int(ent_reason)
        self.status = int(ent_status)
        
        _log.debug('Message(): done.')


    def __repr__(self):
#        usecs = int(1e6*(self.time-int(self.time)))
#        return 'Message(%s, %s:%s, %s, 0x%x, %s, %d:%s, %s, %s)' % (
#            _time.strftime('%%Y%%m%%d %%H:%%M:%%S.%06d %%Z' % (usecs),
#                           _time.localtime(self.time) ),
        return 'Message(%s:%s, %s, 0x%x, %s, %d:%s, %s, %s, %s)' % (
            self.task, self.entry,
            self.name, self.transid,
            _entry_reason_string[self.reason],
            self.status, get_status_string(self.status),
            self.arg_name, self.arg, self.arg_extra)


class TransId:
    '''
    TransId object holds a DitsTransIdType as a python integer.
    Returned from OBEY and such.
    You can wait() to invoke DitsActionTransIdWait.
    '''
    def __init__(self, transid):
        self.transid = transid

    def __eq__(self, other):
        return self.transid == other
    
    def __hash__(self):
        return hash(self.transid)

    def wait(self, seconds=None, dontblock=False):
        '''
        Wait up to 'seconds' for a message on this transid.
        If 'seconds' is None (default), no timeout (wait forever).
        Return Message instance.
        
        If 'dontblock' is True, use the DITS_M_AW_DONT_BLOCK flag.
        Returns a Message if one is already waiting, otherwise returns None.
        Used to clear out the message queue at action end.
        
        TODO: handle absolute timestamps, ala reschedule().
        '''
        cdef DitsTransIdType ctransid = <DitsTransIdType>(<ulong>self.transid)
        cdef DitsDeltaTimeType delay
        cdef DitsDeltaTimeType *delayptr = NULL
        cdef StatusType status = 0
        cdef int count = 0
        cdef int flags = 0
        if dontblock:
            flags = DITS_M_AW_DONT_BLOCK
        if seconds is not None:
            s = int(seconds)
            u = int(1e6*(seconds-s))
            delayptr = &delay
            DitsDeltaTime(s, u, delayptr)
        DitsActionTransIdWait(flags, delayptr, ctransid, &count, &status)
        if status:
            raise BadStatus(status, "DitsActionTransIdWait")
        if count < 0:  # only if dontblock and no waiting messages
            return None
        # since this mechanism bypasses dispatcher for the current action,
        # duplicate the monitor-handling code for automatic cleanup later.
        msg = Message()
        _log.debug('wait(%s,%s) msg: %s', seconds, dontblock, msg)
        # bug? an EXIT during a wait() will have DITS_REA_OBEY.
        if msg.reason == DITS_REA_OBEY:
            raise Exit('DITS_REA_OBEY in TransId.wait')
        if msg.reason == DITS_REA_TRIGGER \
            and msg.status == DITS__MON_STARTED \
            and 'MONITOR_ID' in msg.arg:
            n = msg.name
            mid = msg.arg['MONITOR_ID']
            _log.debug('wait: _monitors[%s].append((%s,%s))', n, msg.task, mid)
            if not n in _monitors:
                _monitors[n] = []
            _monitors[n].append((msg.task, mid))
        return msg


def wait(seconds=None, dontblock=False):
    '''
    Wait up to 'seconds' for a message for this action.
    If 'seconds' is None (default), no timeout (wait forever).
    Return Message instance.
    
    If 'dontblock' is True, use the DITS_M_AW_DONT_BLOCK flag.
    Returns a Message if one is already waiting, otherwise returns None.
    Used to clear out the message queue at action end.
    
    TODO: handle absolute timestamps, ala reschedule().
    '''
    return TransId(0).wait(seconds, dontblock)


cdef class Path:
    '''
    Path object holds a DitsPathType for internal use by obey etc.
    '''
    cdef DitsPathType path

    def __init__(self, task, seconds=0.5):
        cdef StatusType status = 0
        cdef DitsTransIdType transid
        self.path = NULL
        try:
            DitsPathGet(task, NULL, 0, NULL, &self.path, NULL, &status)
            if status == 0:
                return
            if seconds <= 0.0:
                raise BadStatus(status, 'DitsPathGet(%s)' % (task))
            ErsAnnul(&status)
            DitsPathGet(task, NULL, 0, &_default_path_info, &self.path, &transid, &status)
            if status != 0:
                raise BadStatus(status, 'DitsPathGet(%s)' % (task))
            t = TransId(int(<ulong>transid))
            msg = t.wait(seconds)
            if msg.reason == DITS_REA_RESCHED:
                raise BadStatus(DITS__APP_TIMEOUT, 'Path(%s) timeout after %g seconds' % (task, seconds))
            elif msg.reason != DITS_REA_PATHFOUND:
                if msg.status != 0:
                    raise BadStatus(msg.status, 'DitsPathGet(%s)' % (task))
                raise BadStatus(DITS__APP_ERROR, 'Path(%s) unexpected message: %s' % (task, msg))
        except BadStatus:
            status = 0
            DitsLosePath(self.path, &status)
            raise


def cache_path(taskname):
    '''
    Calls Path(taskname), returns nothing.  DRAMA will cache the path,
    helping ensure that future calls to get_path with the same taskname
    will return without waiting.
    '''
    Path(taskname)


def obeykick_impl(o, tid, task, action, *args, **kwargs):
    '''Invoke task:action with given args, transaction optional.'''
    cdef StatusType status = 0
    cdef DitsTransIdType transid = NULL
    cdef DitsTransIdType *transidptr = NULL
    if tid:
        transidptr = &transid
    p = Path(task)
    argid = make_argument(*args, **kwargs)
    if o:
        what = 'DitsObey'
        DitsObey(p.path, action, argid, transidptr, &status)
    else:
        what = 'DitsKick'
        DitsKick(p.path, action, argid, transidptr, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, what + "(%s,%s,%d)" % (task, action, argid))
    return TransId(int(<ulong>transid))


def obey(task, action, *args, **kwargs):
    '''Invoke task:action with given args and return TransId.'''
    return obeykick_impl(True, True, task, action, *args, **kwargs)


def blind_obey(task, action, *args, **kwargs):
    '''Invoke task:action with given args without creating a transaction.'''
    obeykick_impl(True, False, task, action, *args, **kwargs)


def kick(task, action, *args, **kwargs):
    '''Kick task:action with given args and return a TransId.'''
    return obeykick_impl(False, True, task, action, *args, **kwargs)


def blind_kick(task, action, *args, **kwargs):
    '''Kick task:action with given args without creating a transaction.'''
    obeykick_impl(False, False, task, action, *args, **kwargs)


def interested():
    '''Calls DitsInterested(DITS_MSG_M_MESSAGE | DITS_MSG_M_ERROR).'''
    cdef StatusType status = 0
    DitsInterested(DITS_MSG_M_MESSAGE | DITS_MSG_M_ERROR, &status)
    if status:
        raise BadStatus(status, "DitsInterested(DITS_MSG_M_MESSAGE|DITS_MSG_M_ERROR")


def monitor(task, param):
    '''Return a monitor TransId on task:param.

    You can save the MONITOR_ID so you can cancel() it later,
    or you can just let the dispatcher cancel the monitor automatically
    when the action ends:

    if msg.reason == DITS_REA_OBEY:
        monid = None
        montid = monitor('TASK', 'PARAM')
    elif msg.reason == DITS_REA_TRIGGER:
        if msg.status == DITS__MON_STARTED:
            if msg.transid == montid
                monid = msg.arg['MONITOR_ID']
        elif msg.status == DITS__MON_CHANGED:
            ...
    elif msg.reason == DITS_REA_KICK:
        cancel('TASK', monid)
        monid = None
    elif msg.reason == DITS_REA_COMPLETE:
        if msg.transid == montid
            monid = None
    '''
    cdef StatusType status = 0
    cdef DitsTransIdType transid
    cdef DitsGsokMessageType message
    p = Path(task)
    argid = make_argument(param)
    message.flags = DITS_M_ARGUMENT | DITS_M_REP_MON_LOSS #| DITS_M_SENDCUR
    if param != "_ALL_":
        message.flags |= DITS_M_SENDCUR
    message.argument = argid
    message.type = DITS_MSG_MONITOR
    strcpy(message.name.n, "START")
    DitsInitiateMessage(0, p.path, &transid, &message, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsInitiateMessage(%s,%s)" % (task, param))
    return TransId(int(<ulong>transid))


def cancel(task, monid):
    '''Send a CANCEL message for the given MONITOR_ID.  No transaction.'''
    if monid is None:
        return
    cdef StatusType status = 0
    cdef DitsGsokMessageType message
    p = Path(task)
    argid = make_argument(monid)
    message.flags = DITS_M_ARGUMENT
    message.argument = argid
    message.type = DITS_MSG_MONITOR
    strcpy(message.name.n, "CANCEL")
    DitsInitiateMessage(0, p.path, NULL, &message, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsInitiateMessage(%s,%d)" % (task, monid))
    # remove the monitor from the global list (regardless of calling action).
    # and in fact remove all instances from all lists, because paranoia.
    for mlist in _monitors.values():
        try:
            while True:
                mlist.remove((task, monid))
        except:
            pass


def get(task, param):
    '''Return a TransId for param in remote task.'''
    cdef StatusType status = 0
    cdef DitsTransIdType transid
    p = Path(task)
    DitsGetParam(p.path, param, &transid, &status)
    if status != 0:
        raise BadStatus(status, 'DitsGetParam(%s,%s)' % (task, param))
    return TransId(int(<ulong>transid))


def signal(action, *args, **kwargs):
    '''
    Construct a Dits argument from *args/**kwargs and call
    DitsSignalByName to send a message to another action in this task.
    
    WARNING: This function causes a BADID error for the received message
    and will kill any action you send it to.  Do not use.
    '''
    cdef StatusType status = 0
    argid = make_argument(*args, **kwargs)
    DitsSignalByName(action, argid, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsSignalByName(%s,%d)" % (action, argid))


def trigger(*args, **kwargs):
    '''Trigger parent action.'''
    cdef StatusType status = 0
    argid = make_argument(*args, **kwargs)
    DitsTrigger(argid, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsTrigger(%d)" % (argid))


def is_active(task, action, timeout=None):
    '''
    Return True if task:action is active, else False.
    For local task, uses DitsActIndexByName + DitsIsActionActive;
    For remote tasks, queries task:HELP for " action (Active)".

    NOTE: This calls interested(), which can result in extra
        MESSAGE/ERROR messages from other transactions.

    NOTE: HELP can return a LOT of messages,
          make sure your buffers are big enough
          to deal with the flood.
    '''
    cdef StatusType status = 0
    cdef long index
    cdef int active
    if task == _taskname:
        DitsActIndexByName(action, &index, &status)
        if status:
            raise BadStatus(status, "DitsActIndexByName(%s)" % (action))
        DitsIsActionActive(index, &active, &status)
        if status:
            raise BadStatus(status, "DitsActIndexByName(%d (%s))" % (index, action))
        return bool(active)
    needle = " %s (Active)" % (action)
    tid = obey(task, "HELP")
    interested()
    found = False
    while True:
        m = tid.wait(timeout)
        if m.reason == DITS_REA_RESCHED:
            raise BadStatus(DITS__APP_TIMEOUT, 'is_active(%s,%s) timeout after %g seconds' % (task, action, timeout))
        elif m.reason != DITS_REA_MESSAGE:
            break
        tn = m.arg['TASKNAME']
        msg = m.arg["MESSAGE"][0]  # MESSAGE is array of |S200
        found = found or (tn == task and msg.find(needle) >= 0)
    return found


def forward():
    '''
    Call MyMsgForward from local ditsmsg.h to relay the
    current message to the action's parent.  Intended
    to handle uninteresting messages
    of the DITS_REA_MESSAGE and DITS_REA_ERROR variety.
    '''
    cdef StatusType status = 0
    MyMsgForward(&status)
    if status:
        raise BadStatus(status, "MyMsgForward()")


def _msgout(m):
    '''
    Calls MsgOut(STATUS__OK, str).
    Sends a DITS_MSG_MESSAGE to initiator of the current action (immediately).
    Also copies message to JIT_MSG_OUT parameter for external monitors.
    '''
    cdef StatusType status = 0
    m = str(m) or ' '  # MsgOut doesn't handle empty strings properly
    MsgOut(&status, m)
    if status != 0:
        raise BadStatus(status, "MsgOut(%s)" % (m) )
    # emulate jit_MsgOut, if task initialized
    #if _fd != -1:
    #    set_param("JIT_MSG_OUT", m)


def _ersrep(e, s=0):
    '''
    Calls ErsRep(ERS_M_NOFMT | ERS_M_HIGHLIGHT, STATUS__OK, str).
    Also copies message to JIT_ERS_OUT parameter (TODO is this correct?)
    Reports an error to initiator of the current action,
    but note that the error message will not be sent until control
    returns to DRAMA (via delay(), wait(), return from action, etc).
    For immediate output, use ersout() instead.

    TODO: Does ERS_M_HIGHLIGHT suffice for red output in JOSCON,
          or do we need to set bad status as well?
    '''
    cdef StatusType status = int(s)
    e = str(e)
    flags = 0  #ERS_M_NOFMT | ERS_M_HIGHLIGHT
    ErsRep(flags, &status, e)
    if status != 0:
        raise BadStatus(status, "ErsRep(%s)" % (e) )
    # I'm not sure if this is correct --
    #   maybe ersrep needs to allow multiple messages to queue up?
    #   maybe MESSAGE *has* to be 200 chars long?
    #if _fd != -1:
    #    set_param("JIT_ERS_OUT", {'TASKNAME': _taskname,
    #                              'MESSAGE': [e],
    #                              'FLAGS': [_numpy.int32(flags)],
    #                              'STATUS': [_numpy.int32(0)]} )


def _ersout(e, s=0):
    '''
    Calls ErsOut(ERS_M_NOFMT | ERS_M_HIGHLIGHT, STATUS__OK, str).
    Also copies message to JIT_ERS_OUT parameter (TODO is this correct?)
    ErsOut is equivalent to ErsRep + ErsFlush.
    Reports an error to initiator of the current action (immediately).

    TODO: Does ERS_M_HIGHLIGHT suffice for red output in JOSCON,
          or do we need to set bad status as well?
    '''
    cdef StatusType status = int(s)
    e = str(e)
    flags = 0  #ERS_M_NOFMT | ERS_M_HIGHLIGHT
    ErsOut(flags, &status, e)
    if status != 0:
        raise BadStatus(status, "ErsOut(%s)" % (e) )
    # TODO is this correct?
    #if _fd != -1:
    #    set_param("JIT_ERS_OUT", {'TASKNAME': _taskname,
    #                              'MESSAGE': [e],
    #                              'FLAGS': [_numpy.int32(flags)],
    #                              'STATUS': [_numpy.int32(0)]} )


def _wrap(s):
    '''
    Splits s by newlines into a list of shorter strings,
    ensuring no string is longer than 160 chars.
    Motivation: MsgOut/ErsOut truncate long strings.
    '''
    s = str(s)
    nlist = s.split('\n')
    slist = []
    for line in nlist:
        while len(line) > 160:
            # try to break the long line on whitespace
            prefix = line[:160].rsplit(None,1)[0]
            slist.append(prefix)
            line = line[len(prefix):]
        slist.append(line)
    return slist;



def msgout(m):
    if _fd >= 0:  # silently ignore if drama not initialized
        mlist = _wrap(m)
        for m in mlist:
            _msgout(m)


def ersrep(e, s=0):
    if _fd >= 0:  # silently ignore if drama not initialized
        elist = _wrap(e)
        for e in elist:
            _ersrep(e,s)


def ersout(e, s=0):
    if _fd >= 0:  # silently ignore if drama not initialized
        elist = _wrap(e)
        for e in elist:
            _ersout(e,s)


def reschedule(seconds=None):
    '''
    Reschedule the action using DitsPutRequest or jitDelayRequest.
    if seconds is False, DITS_REQ_END (cancel previous reschedule)
    if seconds is None, DITS_REQ_SLEEP (wait for message)
    if seconds <= 0, DITS_REQ_STAGE (reschedule immediately)
    Otherwise call jitDelayRequest.
    'seconds' can be an absolute or relative timeout.
    '''
    cdef StatusType status = 0
    if seconds is False:
        DitsPutRequest(DITS_REQ_END, &status)
    if seconds is None:
        DitsPutRequest(DITS_REQ_SLEEP, &status)
    else:
        s = float(seconds)
        if s > 315360000.0:  # 10*365*86400
            s = s - _time.time()
        if s <= 0.0:
            DitsPutRequest(DITS_REQ_STAGE, &status)
        else:
            jitDelayRequest(s, &status)
    if status != 0:
        raise BadStatus(status, 'reschedule(%s)' % (seconds))
    _rescheduled[_action_stack[-1]] = (seconds is not False)


def rescheduled():
    '''
    Return True if the current action has been rescheduled.
    Return False if not rescheduled, prior rescheduling was cancelled,
      or if this function is called outside an action.
    '''
    return bool(_action_stack and _rescheduled[_action_stack[-1]])


cdef void dispatcher(StatusType *status):
    '''C entry point for all registered DRAMA actions.'''
    cdef StatusType tstatus = 0
    
    _log.debug('dispatcher called.')

    # bad entry status or failing to get entry details is a FATAL error
    n = None  # action name (msg.name), used frequently
    try:
        msg = Message()  # grabs Dits entry info
        _log.debug("dispatcher entry message: %s", msg)
        n = msg.name
    except (TypeError, ValueError):
        status[0] = DITS__INVARG
        _log.exception('dispatcher: invalid arg getting entry details')
    except BadStatus as e:
        status[0] = e.status or DITS__APP_ERROR
        _log.exception('dispatcher: bad status getting entry details: %s', e)
    except:
        status[0] = DITS__APP_ERROR
        _log.exception('dispatcher: error getting entry details')
    finally:
        if status[0] != 0:
            if n is not None:
                bs = BadStatus(status[0], '%s: bad status on entry, sending %s EXIT' % (n, _taskname))
                _log.critical('%s', bs)
            #DitsPutRequest(DITS_REQ_EXIT, &tstatus)  # doesn't work
            DitsPutRequest(DITS_REQ_END, &tstatus)
            blind_obey(_taskname, "EXIT")
            return

    # intercept MON_STARTED to update global _monitors
    if msg.reason == DITS_REA_TRIGGER \
        and msg.status == DITS__MON_STARTED \
        and 'MONITOR_ID' in msg.arg:
        mid = msg.arg['MONITOR_ID']
        _log.debug('dispatcher: _monitors[%s].append((%s,%s))', n, msg.task, mid)
        if not n in _monitors:
            _monitors[n] = []
        _monitors[n].append((msg.task, mid))

    try:
        _action_stack.append(n)
        _activity[n] = _activity.get(n,0) + 1
        _rescheduled[n] = False
        _log.debug('dispatcher: calling action %s: %s', n, _actions[n])
        r = _actions[n](msg)
        _log.debug('dispatcher: action %s returned %s', n, r)
        if r is not None:  # action returned a value
            if isinstance(r, tuple):
                a = make_argument(*r)  # {'Argument1':r[0], 'Argument2':r[1], ...}
            elif isinstance(r, dict):
                a = sds_from_obj(r, 'ArgStructure')  # dict becomes arg
            else:
                a = make_argument(r)  # {'Argument1':r}
            DitsPutArgument(a, DITS_ARG_DELETE, status)
            if status[0]:
                raise BadStatus(status[0], "DitsPutArgument(%s)" % (r))
    except (TypeError, ValueError):
        status[0] = DITS__INVARG
        _log.exception('%s: invalid argument', n)
    except BadStatus as e:
        status[0] = e.status or DITS__APP_ERROR
        _log.exception('%s: bad status', n)
    except Exit as e:
        status[0] = DITS__EXITHANDLER
        _log.debug('%s: raised %r', n, e)
        # Exit now sends EXIT on creation
        #blind_obey(_taskname, "EXIT")  # DITS_REQ_EXIT doesn't work
    except:
        status[0] = DITS__APP_ERROR
        _log.exception('%s: other error', n)
    finally:
        if status[0] != 0:
            _rescheduled[n] = False
        if not _rescheduled[n]:
            DitsPutRequest(DITS_REQ_END, &tstatus)
            if _activity[n] == 1:  # no more nested calls of this action
                # clear out any queued messages
                while wait(dontblock=True) is not None:
                    pass
                # cancel any remaining monitors
                if n in _monitors:
                    mlist = _monitors[n]
                    while mlist:
                        try:
                            task, monid = mlist.pop()
                            cancel(task, monid)
                        except:
                            pass
                    del _monitors[n]
        _action_stack.pop()
        _activity[n] -= 1
    _log.debug('dispatcher done.')


cdef void orphan_handler(StatusType *status):
    '''C orphan transaction handler, cleans up stray monitors.'''
    cdef DitsGsokMessageType message
    cdef DitsPathType path
    msg = Message()  # grabs Dits entry info
    _log.debug('orphan_handler entry message: %s', msg)
    # TODO spit out an ErsRep or something?  where would it go?
    if msg.reason == DITS_REA_TRIGGER and msg.status == DITS__MON_STARTED:
        monid = int(msg.arg['MONITOR_ID'])
        _log.debug('orphan_handler: canceling monitor %s:%d', msg.task, monid)
        path = DitsGetEntPath()
        argid = make_argument(monid)
        message.flags = DITS_M_ARGUMENT
        message.argument = argid
        message.type = DITS_MSG_MONITOR
        strcpy(message.name.n, "CANCEL")
        DitsInitiateMessage(0, path, NULL, &message, status)
        delete_sds(argid)
        if status[0] != 0:
            raise BadStatus(status[0], 'orphan_handler:' + \
                            'DitsInitiateMessage(%s,%d)' % (msg.task, monid) )


def init( taskname,
          flags = DITS_M_X_COMPATIBLE, # | DITS_M_IMB_ROUND_ROBIN,  # RR not in Hilo yet
          buffers = [32000, 8000, 8000, 2000],
          tidefile = None,
          actions = [] ):
    '''
    Creates a new DRAMA task.
        taskname: Name of the task, required.
        flags: Default: DITS_M_X_COMPATIBLE | DITS_M_IMB_ROUND_ROBIN.
            DITS_M_X_COMPATIBLE will always be set even if you
            pass in flags=0, as it is required for the custom loop impl.
        buffers: A list of DRAMA buffer sizes,
            [taskbytes, sendbytes, recvbytes, selfbytes].
            Default: [32000, 8000, 8000, 2000].
            Note that these values will be overridden if your taskname
            is specified in jit_tasks.xml.
        tidefile: File name to pass to tideInit() (default None).
            An empty string will cause tideInit() to use an
            init file from the default location (TODO where/what).
        actions: A list of functions to use as actions.
            Action names are the same as the function names,
            so be sure to follow the DRAMA naming conventions.
            If you need custom action names, use the register_action()
            function instead.
    '''
    cdef StatusType status = 0
    taskname = str(taskname)

    # make sure global environment is cleaned up
    stop(taskname)

    # flags must include X_COMPATIBLE for select() loop to work,
    #   and must include M_SELF_BYTES for TIDE to work.
    flags |= DITS_M_X_COMPATIBLE | DITS_M_SELF_BYTES
    _log.debug('init: flags: %d = 0x%x', flags, flags)

    #jitSetDefaults( flags, 0.0, *buffers, &status )
    b = buffers
    _log.debug('init: buffer sizes: %s', b)
    jitSetDefaults(flags, 0.0, b[0], b[1], b[2], b[3], &status)
    if status != 0:
        raise BadStatus(status, "jitSetDefaults")

    # manually set our own default path info ala jitSetDefaults
    _default_path_info.MessageBytes = 8000
    _default_path_info.MaxMessages = 1
    _default_path_info.ReplyBytes = 8000
    _default_path_info.MaxReplies = 1
    if b[1] > 0:
        _default_path_info.MessageBytes = b[1]
    if b[2] > 0:
        _default_path_info.ReplyBytes = b[2]

    jitAppInit (taskname, &status)
    if status != 0:
        raise BadStatus(status, "jitAppInit(%s)" % (taskname) )
    # special flag that it is safe to call jitStop() now
    global _fd, _taskname
    _fd = -2
    _taskname = taskname

    DitsPutOrphanHandler(orphan_handler, &status)
    if status != 0:
        raise BadStatus(status, "DitsPutOrphanHandler")

    # register global _actions{} using the C dispatcher() function
    if actions:
        for action in actions:
            register_action(action.__name__, action)

    if tidefile is not None:
        _log.debug('init: tideInit(%s)', tidefile)
        # connection problems can occur if caRepeater is not already running
        try:
            import subprocess
            subprocess.Popen('caRepeater')
            _time.sleep(.05)
        except:
            pass
        tideInit(&_altin, tidefile, &status)
        if status != 0:
            raise BadStatus(status, "tideInit(%s)" % (tidefile) )
        _altin.exit_flag = 0
        _log.debug('init: saved altin: 0x%lx', <ulong>_altin)

    acts = 'init: %s actions:' % (taskname)
    for k,v in _actions.items():
        acts += '\n  %s : %s' % (k,v)
    _log.debug(acts)
    # init


def register_action(name, action):
    '''
    Register a Python callable object as a DRAMA action.
        name: Action name, must follow DRAMA naming conventions.
        action: Callable object (function, bound member function, etc).
    '''
    cdef StatusType status = 0
    cdef DitsActionDetailsType details
    if len(name) > DITS_C_NAMELEN:
        raise ValueError('name too long (%d chars) for %s: %s' %  \
                         (len(name), action, name) )
    memset(&details, 0, sizeof(details))
    details.obey = dispatcher
    details.kick = dispatcher
    strcpy(details.name, name)
    DitsPutActions(1, &details, &status)
    if status != 0:
        raise BadStatus(status, "DitsPutActions(%s)" % (name) )
    _actions[name] = action


def register_callback(fd, callback):
    '''
    Register a callable object (function, bound member function, etc)
    to be invoked as callback(fd) when fd becomes available for reading.
    To unregister an fd, pass callback=None.
    '''
    if not callback:
        if fd in _callbacks:
            del _callbacks[fd]
    else:
        _callbacks[fd] = callback


def get_fd_sets():
    '''Return a tuple of (read,write,except) sets of registered fd's.'''
    cdef StatusType status = 0
    cdef long xcond = 0  # MUST be a long to match size of void** cast
    r,w,x = set(), set(), set()

    DitsGetXInfo(&_fd, <void**>&xcond, &status)
    if status != 0:
        raise BadStatus(status, "DitsGetXInfo")
    if (xcond & XtInputReadMask):
        r.add(_fd)
    if (xcond & XtInputWriteMask):
        w.add(_fd)
    if (xcond & XtInputExceptMask):
        x.add(_fd)

    if _altin:
        _log.debug('get_fd_sets: _altin loop')
        for i in xrange(DITS_C_ALT_IN_MAX):
            if _altin.Array[i].number < 0:
                continue
            _log.debug('get_fd_sets: _altin[%d] number=%d, condition=0x%x',
                        i, _altin.Array[i].number, _altin.Array[i].condition)
            if (_altin.Array[i].condition & DITS_M_READ_MASK):
                r.add(_altin.Array[i].number)
            if (_altin.Array[i].condition & DITS_M_WRITE_MASK):
                w.add(_altin.Array[i].number)
            if (_altin.Array[i].condition & DITS_M_EXCEPT_MASK):
                x.add(_altin.Array[i].number)

    # user callbacks available for read fd's only
    for fd in _callbacks.keys():
        r.add(fd)

    return r,w,x


def process_fd(fd):
    '''
    Invoke callback(s) for this file descriptor.
    Raises Exit if handler detects an exit condition.
    '''
    cdef StatusType status = 0
    cdef long exit_flag = 0

    global _g_exit_flag

    # this comparison fails for e.g. socket objects.  don't bother.
    #if fd < 0:
    #    # TODO raise exception?
    #    return
    
    _log.debug('process_fd(%s)', fd)

    # Is the main Dits fd?
    if fd == _fd:
        _log.debug('process_fd: calling DitsMsgAvail')
        msg_count = DitsMsgAvail(&status)
        _log.debug('process_fd: msg_count %d', msg_count)
        while not _g_exit_flag and not exit_flag and msg_count > 0:
            _log.debug('process_fd: calling DitsMsgReceive')
            DitsMsgReceive(&exit_flag, &status)
            _log.debug('process_fd: calling DitsMsgAvail')
            msg_count = DitsMsgAvail(&status)
            _log.debug('process_fd: msg_count %d', msg_count)
        if status:
            raise BadStatus(status, 'process_fd: DitsMsgReceive')
        if exit_flag:
            raise Exit('process_fd: DitsMsgReceive')
        if _g_exit_flag:
            raise Exit('process_fd: _g_exit_flag')
        return

    # Is it a user-callback registered fd?
    if fd in _callbacks:
        _callbacks[fd](fd)
        return

    # Is it a TIDE fd?
    if _altin:
        for i in xrange(DITS_C_ALT_IN_MAX):
            if fd == _altin.Array[i].number:
                _log.debug('process_fd: _altin.Array[%d].routine', fd)
                _altin.Array[i].routine(_altin.Array[i].client_data, &status)
                if status or _altin.exit_flag:
                    s = 'TIDE routine, fd %d, index %d' % (fd, i)
                    if _altin.exit_flag:
                        if status:
                            raise BadStatus(status, s)
                        raise Exit(s)
                    # ditsaltin.c ignores non-exit status errors
                    bs = BadStatus(status, s)
                    _log.warn('%s', bs)
                    ErsClear(&status)


# Cython doesn't play nice with function attributes as static variables
run_loop_depth = -1

def run_loop(tk, timeout_seconds, return_dits_msg=False):
    '''
    The message-checking loop used by run() and wait().
    For wait(), return_dits_msg=True.  This will cause a Dits message
    to break the loop so wait() can return to the calling action.
    '''
    cdef StatusType status = 0
    global run_loop_depth, _g_exit_flag
    
    try:
        run_loop_depth += 1
        
        # process dits messages immediately in wait() loops
        if return_dits_msg:
            msg_count = DitsMsgAvail(&status)
            # TODO we might just need to ignore these
            if status:
                raise BadStatus(status, 'run_loop(%d) DitsMsgAvail'%(run_loop_depth))
            if msg_count:
                _log.debug('run_loop(%d): msg_count %d, return_dits_msg', run_loop_depth, msg_count)
                return
        
        looping = True
        while looping and not _g_exit_flag:
            # fd's might change, must check every time
            _log.debug('run_loop(%d): calling get_fd_sets()', run_loop_depth)
            r,w,x = get_fd_sets()
            sr,sw,sx = [],[],[]
            _log.debug('run_loop(%d): select(%s,%s,%s,%s)', run_loop_depth, r,w,x,timeout_seconds)
            try:
                sr,sw,sx = _select.select(r,w,x, timeout_seconds)
            except _select.error as e:
                # we can ignore 'Interrupted system call'
                if e.args[0] != _errno.EINTR:
                    _log.info('Exception: {}, errno={}'.format(e, _errno.errorcode[e.args[0]]))
                    raise
            fds = set(sr) | set(sw) | set(sx)
            _log.debug('run_loop(%d): fds %s', run_loop_depth, fds)
            for fd in fds:
                if return_dits_msg and fd == _fd:
                    _log.debug('run_loop(%d): return_dits_msg', run_loop_depth)
                    looping = False
                else:
                    _log.debug('run_loop(%d): process_fd(%s)', run_loop_depth, fd)
                    process_fd(fd)
            if tk is not None:
                _log.debug('run_loop(%d): %r.update()', run_loop_depth, tk)
                tk.update()
    finally:
        run_loop_depth -= 1
    
    # run_loop


cdef void event_wait_handler(void *client_data, StatusType *status):
    '''
    C function registered with DitsPutEventWaitHandler
    to invoke run_loop() during a wait().
    
    Note we call EXIT manually in the exception handlers since
    exceptions probably won't escape this C function.
    '''
    if status[0]:
        return
    args = <object><PyObject*>client_data
    tk, timeout_seconds, TclError = args
    try:
        _log.debug('event_wait_handler: run_loop')
        run_loop(tk, timeout_seconds, True)
    except Exit as e:
        status[0] = DITS__EXITHANDLER
        # Exit now sends EXIT on creation
        #blind_obey(_taskname, "EXIT")
        _log.debug('event_wait_handler caught %r', e)
    except TclError as e:
        status[0] = DITS__EXITHANDLER
        _log.debug('event_wait_handler caught %r, sending %s EXIT', e, _taskname)
        blind_obey(_taskname, "EXIT")
        if e.args[0].find('application has been destroyed') < 0:
            _log.exception('event_wait_handler')
    except:
        status[0] = DITS__EXITHANDLER
        _log.exception('event_wait_handler sending %s EXIT', _taskname)
        blind_obey(_taskname, "EXIT")
        # raise?  it'll probably be ignored anyway
        raise


# tk <Destroy> event callback to detect window close.
_destroy_once = 1
def on_destroy(event):
    global _destroy_once
    if _destroy_once:
        _destroy_once = 0
        Exit('tk destroyed')  # no need to raise
            

def run(tk=None, hz=50):
    '''
    Run the custom DRAMA event loop.
    Do not use this function if you need to use an external event loop!
    The drama_qt4.DramaWidget base class, for instance, creates a
    QSocketNotifier for every fd in get_fd_sets() and calls process_fd()
    as each fd becomes available.

    Will run a Tk loop if you've imported the Tkinter module.
    Periodically calls tk.update() in a select() loop
        tk: Tk() instance, if None will use Tkinter._default_root.
        hz: select() loop (GUI) update rate, default 50Hz.

    Note that (thanks to duck-typing) you could pass any object with
    update() and bind() method as 'tk', which might be useful if you want
    something called periodically and don't want to set up an action for it.
    '''
    cdef StatusType status = 0

    # Tkinter is a big module, don't import unnecessarily
    if 'Tkinter' in _sys.modules:
        _log.debug('run: using Tkinter')
        import Tkinter as _Tkinter
        TclError = _Tkinter.TclError
        if tk is None:
            tk = _Tkinter._default_root
    elif 'tkinter' in _sys.modules:  # there's probably a better way
        _log.debug('run: using tkinter')
        import tkinter as _tkinter
        TclError = _tkinter.TclError
        if tk is None:
            tk = _tkinter._default_root
    else:
        TclError = None  # 'except None as e' is okay

    timeout_seconds = None
    if tk is not None:
        timeout_seconds = 1.0/hz
        _log.debug('run: calling %r.update() every %g seconds', tk, timeout_seconds)
        # ...starting now, since the first update() can be very slow.
        tk.bind("<Destroy>", on_destroy)
        tk.update()
    
    _log.debug('run: DitsPutEventWaitHandler')
    client_data = [tk, timeout_seconds, TclError]
    DitsPutEventWaitHandler(event_wait_handler, <void*><PyObject*>client_data, &status)
    if status:
        raise BadStatus(status, 'DitsPutEventWaitHandler')

    try:
        _log.debug('run: run_loop')
        run_loop(tk, timeout_seconds)
    except Exit as e:
        # catch Exit() so it doesn't cause bad $? exit status
        _log.debug('run caught %r', e)
        pass
    except TclError as e:
        _log.debug('run caught %r', e)
        # exit quietly if tk destroyed, else reraise
        if e.args[0].find('application has been destroyed') < 0:
            raise
    
    _log.debug('run: done.')


def stop(taskname=None):
    '''
    If called from inside an action, raises Exit.

    Otherwise, cleans up DRAMA globals and calls jitStop(taskname).
    If taskname==None, use the global _taskname from init().

    You MUST call this function to make sure the task is unregistered
    from the IMP network system; a finally: block is a good place for it.
    '''
    cdef StatusType status = 0
    global _taskname, _altin, _fd, _callbacks, _actions, _monitors

    if _action_stack:
        raise Exit('stop')

    for mlist in _monitors.values():
        while mlist:
            try:
                task, monid = mlist.pop()
                _log.debug('stop: cancel(%s, %s)', task, monid)
                cancel(task, monid)
            except:
                pass
    _monitors = {}

    # Must clean up _altin manually; tideExit() is not enough.
    if _altin:
        _log.debug('stop: calling tideExit()')
        tideExit(&status)
        DitsFree(_altin)
        _altin = NULL
        if status != 0:
            bs =  BadStatus(status, "tideExit")
            _log.warn('%s', bs)
            status = 0

    # Use _fd as a guard to avoid jitStop() segfaults.
    if _fd != -1:
        if taskname is None:
            taskname = _taskname
        _log.debug('stop: calling jitStop(%s)', taskname)
        jitStop(taskname, &status)
        _fd = -1
        if status != 0:
            bs = BadStatus(status, "jitStop(%s)" % (taskname) )
            _log.warn('%s', bs)

    # Clean up remaining globals.
    _callbacks = {}
    _actions = {}


