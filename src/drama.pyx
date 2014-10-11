#cython: embedsignature=True
'''
DRAMA Python module.

Author: Ryan Berthold, JAC

A note on logging/debug output:
    
    This module uses the standard python 'logging' module, not jitDebug.
    It uses __name__ for the logger name, which should be 'drama'.
    A NullHandler is installed, so you don't need to configure logging
    if you don't want to.
    
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
    
     
TODO: tideSetParam for pushing parameter updates back to EPICS space.

'''


from drama cimport *

import sys as _sys
import time as _time
import select as _select
import errno as _errno
import greenlet as _greenlet
import numpy as _numpy
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

# File descriptor callbacks, {read_fd:func(fd)}.  These are NOT ACTIONS.
_callbacks = {}

# Registered task actions, {name:func}.
_actions = {}

# Greenlets for active actions, {name:greenlet}.
_greenlets = {}

# Actions must all be started from the same dispatcher context.
# If dispatcher is ever called from within an action
# (due to a DitsActionWait or something) things get really fouled up,
# so we consider that a fatal error.
_dispatcher_greenlet = None

# Logging config is left for the user
#_log = _logging.getLogger(__name__)  # drama.__drama__, not great
_log = _logging.getLogger('drama')
_log.addHandler(_logging.NullHandler())  # avoid 'no handlers' exception


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


def get_status_string(status):
    '''Return the message string for a numeric status code.'''
    cdef char buf[256]
    MessGetMsg(status, -1, sizeof(buf), buf)
    return str(buf).replace('%', '')


class DramaException(Exception):
    '''Common base class for DITS exceptions'''
    pass


class Kicked(DramaException):
    '''Raised on DITS_REA_KICK, saves kick message.'''
    def __init__(self, message):
        self.message = message  # should be a Message() instance
        self.args = (message,)


class Died(DramaException):
    '''Raised on DITS_REA_DIED, saves transaction object for dead task.'''
    def __init__(self, transaction):
        self.transaction = transaction  # should be a Transaction() instance
        self.message = transaction
        self.args = (transaction,)


class Unexpected(DramaException):
    '''Raised on unexpected/unhandled message, saves said message.'''
    def __init__(self, message):
        self.message = message  # should be a Message() instance
        self.args = (message,)


class Timeout(DramaException):
    '''Raised when wait() times out, saves waited seconds + transactions.'''
    def __init__(self, seconds=None, transactions=[]):
        self.seconds = seconds
        self.transactions = transactions
        self.message = (seconds, transactions)
        self.args = (seconds, transactions)


class Exit(DramaException):
    '''Raised by process_fd() for normal exit; standard string message.'''
    pass


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
    cdef SdsIdType cid
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
    '''
    cdef SdsIdType id = 0
    cdef StatusType status = 0
    cdef ulong cdims[7]
    cdef ulong cindex[7]

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
            kid = sds_from_obj(obj[k], k, id)
            SdsFreeId(kid, &status)
        return id
    else:
        # cast whatever it is to a numpy array, query dtype.
        # this can end up casting everything to strings :/
        obj = _numpy.array(obj)
        dtype = str(obj.dtype)
        shape = obj.shape

    # for strings, append strlen to dims; get non-struct typecode
    if dtype.startswith("|S"):
        # some DRAMA ops expect null-terminated strings, but
        # python strings usually aren't.
        slen = int(dtype[2:])
        maxlen = _numpy.max([len(x) for x in obj.flat])
        if maxlen == slen:  # no space for \0
            slen += 1
            dtype = '|S%d' % (slen)
            obj = _numpy.array(obj, dtype=dtype)
        shape = list(shape)
        shape.append(slen)
        code = SDS_CHAR
    elif dtype != 'object':
        code = _dtype_to_sds_code[dtype]

    # reverse numpy dim order for dits
    for i in xrange(len(shape)):
        cdims[i] = shape[-(1+i)]
    
    if dtype == 'object':
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
            kid = sds_from_obj(obj[index], name, 0)
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
    obuf = obj.tostring()
    SdsPut(id, obj.nbytes, 0, <char*>obuf, &status)
    if status != 0:
        # NOTE obuf could be huge, so first 16 chars only :/
        dots = ''
        if len(obuf) > 16:
            dots = '...'
        raise BadStatus(status, "SdsPut(%d,%d,0,%s%s)" % \
                        (id, obj.nbytes, obuf[:16], dots) )
    return id


def obj_from_sds(id):
    '''Given an SDS id, recursively construct and return a python object.'''
    cdef StatusType status = 0
    cdef SdsIdType cid
    cdef ulong cindex[7]
    cdef void* buf
    cdef ulong buflen

    if id == 0:
        return None
    
    name, code, dims = sds_info(id)

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
                SdsIndex(id, i, &cid, &status)
                if status != 0:
                    break
                cname, dummy, dummy = sds_info(cid)
                obj[cname] = obj_from_sds(cid)
                i += 1
                SdsFreeId(cid, &status)
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
                obj[index] = obj_from_sds(cid)
                SdsFreeId(cid, &status)
            return obj

    # for anything else we need the raw buffer
    SdsPointer(id, &buf, &buflen, &status)
    if status == SDS__UNDEFINED:
        return None
    if status != 0:
        raise BadStatus(status, "SdsPointer(%d)" % (id))
    sbuf = PyString_FromStringAndSize(<char*>buf, buflen)

    # using a string as a buffer is problematic because strings are immutable
    # and numpy decides to use the buffer memory directly.
    # NOTE use .copy() to force array memory ownership.

    if code == SDS_CHAR:
        if dims is None or len(dims) < 2:
            n = sbuf.find('\0')
            if n >= 0:
                sbuf = sbuf[:n]
            return sbuf
        dtype = '|S%d' % (dims[-1])
        obj = _numpy.ndarray(shape=dims[:-1], dtype=dtype, buffer=sbuf).copy()
        # clean up the strings so they look nicer when printed;
        # trailing garbage will show up if non-null.
        for index in _numpy.ndindex(obj.shape):
            n = obj[index].find('\0')
            if n >= 0:
                obj[index] = obj[index][:n]
        return obj

    dtype = _sds_code_to_dtype[code]
    obj = _numpy.ndarray(shape=dims, dtype=dtype, buffer=sbuf).copy()
    obj = obj[()]  # this will deref a scalar array or return original array.
    return obj


def sds_from_xml(buf):
    '''Return a new SDS structure id from XML buf (data or filename).'''
    cdef SdsIdType id = 0
    cdef StatusType status = 0
    jitXML2Sds(len(buf), buf, &id, &status)
    if status != 0:
        raise BadStatus(status, "jitXML2Sds(%s)" % (buf))
    return id


def obj_from_xml(buf):
    '''Return python object parsed from XML buf (data or filename).'''
    id = sds_from_xml(buf)
    obj = obj_from_sds(id)
    delete_sds(id)
    return obj


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
    '''
    if arg is None:
        return [],{}
    elif isinstance(arg, list):
        return arg,{}
    elif not isinstance(arg, dict):
        return [arg],{}
    kwargs = arg
    pargs = {}
    for k in kwargs.keys():
        if k.startswith('Argument'):
            pargs[int(k[8:])] = kwargs[k]
            del kwargs[k]
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


class Fifo:
    '''
    Simple first-in, first-out queue.
    Add newest item with 'push', retrieve oldest item with 'pop'.
    Items are listed oldest to newest in string representation.
    Amortized-constant push/pop, but no iteration or random access.
    '''
    
    def __init__(self):
        self._in = []
        self._out = []
    
    def __repr__(self):
        return 'Fifo(%s)' % str(list(reversed(self._out)) + self._in)
    
    def __len__(self):
        return len(self._in) + len(self._out)
    
    def push(self, item):
        '''Add newest item to the back of the Fifo.'''
        self._in.append(item)
    
    def pop(self):
        '''Remove and return oldest item from the front of the Fifo.'''
        if not self._out:
            self._out = self._in
            self._out.reverse()
            self._in = []
        return self._out.pop()

    def peek_newest(self):
        '''Return (but do not remove) newest item from back of Fifo.'''
        if self._in:
            return self._in[-1]
        return self._out[0]
    
    def peek_oldest(self):
        '''Return (but do not remove) oldest item from front of Fifo.'''
        if self._out:
            return self._out[-1]
        return self._in[0]

    def clear(self):
        '''Remove all items from Fifo, return nothing.'''
        self.__init__()


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
        arg_list  [], positional args from DitsGetArgument()
        arg_dict  {}, keyword args from DitsGetArgument()
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
        DitsGetEntInfo (DITS_C_NAMELEN, ent_name, &ent_path, &ent_transid,
                        &ent_reason, &ent_status, &status)
        if status != 0:
            raise BadStatus(status, "DitsGetEntInfo")

        # entry name is not necessarily the action name.
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
            ent_path = DitsGetParentPath()
        # get taskname if non-NULL path, otherwise shrug and give up
        if <ulong>ent_path != 0:
            DitsTaskFromPath(ent_path, DITS_C_NAMELEN, ent_task, &status)
            if status != 0:
                raise BadStatus(status,
                                "DitsTaskFromPath(0x%x)" % (<ulong>ent_path) )
        else:
            strcpy(ent_task, "???")

        # get message argument, separate positional/keyword parameters
        argid = DitsGetArgument()
        arg = obj_from_sds(argid)
        
        self.arg_name = None
        self.arg_extra = None
        if argid != 0:
            self.arg_name, arg_code, arg_dims = sds_info(argid)
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
        self.arg_list, self.arg_dict = parse_argument(arg)

    def __repr__(self):
#        usecs = int(1e6*(self.time-int(self.time)))
#        return 'Message(%s, %s:%s, %s, 0x%x, %s, %d:%s, %s, %s)' % (
#            _time.strftime('%%Y%%m%%d %%H:%%M:%%S.%06d %%Z' % (usecs),
#                           _time.localtime(self.time) ),
        return 'Message(%s:%s, %s, 0x%x, %s, %d:%s, %s, %s, %s, %s)' % (
            self.task, self.entry,
            self.name, self.transid,
            _entry_reason_string[self.reason],
            self.status, get_status_string(self.status),
            self.arg_name, self.arg_list, self.arg_dict, self.arg_extra)


def absolute_timeout(seconds):
    '''
    Given seconds, return an absolute timeout based on current time.time().
    If seconds is None, return None.
    If seconds is >10yr, assume it is already
    an absolute timeout and return it unchanged.
    '''
    if seconds is None:
        return seconds
    seconds = float(seconds)
    if seconds > 315360000.0:  # 10 * 365 * 86400
        return seconds
    return _time.time() + seconds


class Transaction:
    '''
    Base class for keeping track of ongoing DRAMA transactions.
    Each instance gets copied to the current action's (greenlet's)
    transaction dictionary.
    
    Instance attributes:
        transid   int, transaction id (address)
        running   bool, True while transaction in progress
        status    int, completion status once running=False
        messages  Fifo<Message>, queue of received messages
    '''
    
    def __init__(self, transid, running=True):
        '''
        By default the Transaction starts with running=True,
        but e.g. Monitors override this since they aren't
        considered 'running' until they get a MONITOR_ID.
        '''
        self.transid = transid
        self.running = running
        self.status = 0
        self.messages = Fifo()
        _greenlet.getcurrent().transactions[self.transid] = self
        _log.debug('created %s' % (self))
    
    def __del__(self):
        _log.debug('~%s' % (self))
        if hasattr(self, 'messages') and self.messages:
            _log.warn('unhandled messages in ~%s: %s' % (self, self.messages))
    
    def __repr__(self):
        return 'Transaction(0x%x)' % (self.transid)

    def wait(self, secs=None):
        '''Synonym for wait(secs, self)'''
        return wait(secs, self)
    
    def join(self, timeout=None):
        '''
        Wait for running==False.  'timeout' can be None (wait forever),
        seconds to wait, or an absolute unix timestamp to wait until
        (seconds since 19700101, like time.time()).
        
        BEWARE: This function calls wait(), which allows other actions
        to run concurrently and can receive messages and raise exceptions
        for ANY of your action's other running Transactions.
        '''
        timeout = absolute_timeout(timeout)
        while self.running:
            wait(timeout, self)


cdef DitsPathType get_path(object task, double timeout=10.0) except NULL:
    '''
    Get a Dits path to a task, waiting up to timeout secs for the Transaction.
    Needed to replace jitPathGet, called inside most jit* functions,
    since it calls ActionWait and creates nested dispatcher calls, very bad.
    
    Set timeout to 0.0 to prevent wait() entirely, raising BadStatus if
    a valid path is not already cached.  Useful for Monitor.cancel(),
    where calling wait() might not be safe.
    
    timeout can be absolute (seconds since 19700101), ala time.time().
    Sorry, there's no 'wait forever' support for this function;
    you'll have to settle for waiting a ridiculously long time.
    
    TODO: NULL is assumed to be an error value for path, but I might
          need to change to 'except? NULL' or 'except *' if NULL is okay.
    '''
    cdef StatusType status = 0
    cdef DitsPathType path = NULL
    cdef DitsTransIdType transid
    DitsPathGet(task, NULL, 0, NULL, &path, NULL, &status)
    if status == 0:
        return path
    if timeout <= 0.0:
        raise BadStatus(status, 'DitsPathGet(%s)' % (task))
    ErsAnnul(&status)
    DitsPathGet(task, NULL, 0, &_default_path_info, &path, &transid, &status)
    if status != 0:
        raise BadStatus(status, 'DitsPathGet(%s)' % (task))
    t = Transaction(int(<ulong>transid))
    t.join(timeout)
    return path


def cache_path(taskname):
    '''
    Calls get_path(taskname), returns nothing.  DRAMA will cache the path,
    helping ensure that future calls to get_path with the same taskname
    (such as in Obey, Monitor, etc) will return without calling wait().
    '''
    get_path(taskname)


class Signal(Transaction):
    '''
    Receives messages from peer actions, basically just a Transaction(0x0).
    Create one of these in your action if you need to handle DITS_REA_ASTINT.
    '''

    def __init__(self):
        # Make sure no duplicate Signal instances for this action
        if _greenlet.getcurrent().transactions.has_key(0):
            raise RuntimeError('Duplicate Signal() handler for this action.')
        Transaction.__init__(self, 0)
    
    def __repr__(self):
        return 'Signal()'


class Obey(Transaction):
    '''
    Invokes an action and monitors its status.
    Instance attributes (besides those from Transaction):
        task     str, name of target task
        action   str, name of target action
    '''
    
    def __init__(self, task, action, *args, **kwargs):
        '''
        Creates a Dits argument structure from *args and **kwargs,
        resolves path, calls DitsObey, and inits Transaction(transid).
        
        WARNING, path resolution may call wait(10), which allows
        other actions to run and can accept messages and raise exceptions
        for ANY of your action's running Transactions.
        '''
        cdef StatusType status = 0
        cdef DitsTransIdType transid
        cdef DitsPathType path
        self.task = task
        self.action = action
        path = get_path(task)
        argid = make_argument(*args, **kwargs)
        DitsObey(path, action, argid, &transid, &status)
        delete_sds(argid)
        if status != 0:
            raise BadStatus(status, "DitsObey(%s,%s,%d)" % \
                                    (task, action, argid) )
        Transaction.__init__(self, int(<ulong>transid))

    def __repr__(self):
        return 'Obey(%s.%s)' % (self.task, self.action)

    def kick(self, *args, **kwargs):
        '''
        Kick the obeyed action with an argument created from *args/**kwargs.
        Uses a NULL transid to avoid duplicate COMPLETE messages,
        but note this means you will not get a MESREJECTED if the
        action is not running.  Use the Kick class instead if you need to
        be sure that the kick succeeds.
        '''
        cdef StatusType status = 0
        cdef DitsPathType path
        _log.debug('kicking %s' % (self))
        path = get_path(self.task, 0.0)  # no wait() allowed here
        argid = make_argument(*args, **kwargs)
        DitsKick(path, self.action, argid, NULL, &status)
        delete_sds(argid)
        if status != 0:
            raise BadStatus(status, "DitsKick(%s,%s,%d)" % \
                            (self.task, self.action, argid) )


class Kick(Transaction):
    '''
    Kicks an action and watches transid for completion.
    Instance attributes (besides those from Transaction):
        task     str, name of target task
        action   str, name of target action
    '''
    
    def __init__(self, task, action, *args, **kwargs):
        '''
        Creates a Dits argument structure from *args and **kwargs,
        resolves path, calls DitsKick, and inits Transaction(transid).
        
        WARNING, path resolution may call wait(10), which allows
        other actions to run and can accept messages and raise exceptions
        for ANY of your action's running Transactions.
        '''
        cdef StatusType status = 0
        cdef DitsTransIdType transid
        cdef DitsPathType path
        self.task = task
        self.action = action
        path = get_path(task)
        argid = make_argument(*args, **kwargs)
        DitsKick(path, action, argid, &transid, &status)
        delete_sds(argid)
        if status != 0:
            raise BadStatus(status, "DitsKick(%s,%s,%d)" % \
                                    (task, action, argid) )
        Transaction.__init__(self, int(<ulong>transid))

    def __repr__(self):
        return 'Kick(%s.%s)' % (self.task, self.action)


class Monitor(Transaction):
    '''
    Creates DRAMA monitors and receives update messages.

    The monitor is created with .running=False until a MONITOR_ID
    message arrives.  This is done to give the user the option to
    create multiple monitors and wait in parallel (and so Monitor.__init__
    doesn't have to deal with possible exceptions that wait() might raise).
    Like so:

        monlist = [Monitor("TASK1", "PARAM1"),
                   Monitor("TASK2", "PARAM2")]
        while not all([x.running for x in monlist]):
            wait(mon_timeout, [x for x in monlist if not x.running])
            
    Note that to cancel the monitor you need to explicitly cancel() it;
    'del mymonitor' won't work because the action will retain a
    reference to the monitor under the hood.  Again, canceling a monitor
    leaves it in the .running=True state until a completion message arrives.

    Monitors are always canceled automatically when an action returns,
    with the final update/completion messages handled by the orphan
    message handler so your action will not remain active in a
    waiting state after you return from it.

    If you monitor parameter _ALL_, current values will not be sent.  Also,
    values from message_value()/value()/pop() will have a {name:value} format,
    since _ALL_ basically puts individual monitors on all the top-level params.
    If checking messages manually, msg.arg_name will hold the param name.
    
    Instance attributes (besides those from Transaction):
        running  bool, True between MONITOR_ID and COMPLETE.
        monid    int, used to cancel the monitor
        task     str, name of target task
        param    str, name of target SDP parameter
    '''

    def __init__(self, task, param):
        '''
        Resolves path, sends a START message, and inits Transaction.
        
        WARNING, path resolution may call wait(10), which allows
        other actions to run and can accept messages and raise exceptions
        for ANY of your action's running Transactions.
        '''
        cdef StatusType status = 0
        cdef DitsTransIdType transid
        cdef DitsPathType path
        cdef DitsGsokMessageType message
        self.monid = None
        self.task = task
        self.param = param
        path = get_path(task)
        argid = make_argument(param)
        message.flags = DITS_M_ARGUMENT | DITS_M_REP_MON_LOSS #| DITS_M_SENDCUR
        if param != "_ALL_":
            message.flags |= DITS_M_SENDCUR
        message.argument = argid
        message.type = DITS_MSG_MONITOR
        strcpy(message.name.n, "START")
        DitsInitiateMessage(0, path, &transid, &message, &status)
        delete_sds(argid)
        if status != 0:
            raise BadStatus(status, "DitsInitiateMessage(%s,%s)" % \
                                    (self.task, param) )
        Transaction.__init__(self, int(<ulong>transid), running=False)

    def __del__(self):
        Transaction.__del__(self)
        self.cancel()

    def __repr__(self):
        return 'Monitor(%s.%s)' % (self.task, self.param)

    def cancel(self):
        '''
        Sends a CANCEL message for this monitor, with NULL transid
        to avoid duplicate messages.  The monitor will
        continue to run until a completion message arrives.
        '''
        cdef StatusType status = 0
        cdef DitsGsokMessageType message
        cdef DitsPathType path
        # order is important, self.running might not exist yet
        if self.monid is not None and self.running:
            _log.debug('canceling %s' % (self))
            path = get_path(self.task, 0.0)  # no wait() allowed here
            argid = make_argument(self.monid)
            message.flags = DITS_M_ARGUMENT
            message.argument = argid
            message.type = DITS_MSG_MONITOR
            strcpy(message.name.n, "CANCEL")
            DitsInitiateMessage(0, path, NULL, &message, &status)
            delete_sds(argid)
            self.monid = None  # make sure we never try cancel again
            if status != 0:
                raise BadStatus(status, "DitsInitiateMessage(%s,%d)" % \
                                        (self.task, self.monid) )

    def raw_message_value(self, m):
        '''
        Given Message m, returns value from m.arg_list/m.arg_dict.
        TODO: make static?
        '''
        if m is None:
            return {}
        if len(m.arg_list) == 1 and len(m.arg_dict) == 0:
            return m.arg_list[0]
        elif len(m.arg_list) == 0:
            return m.arg_dict
        # for anything else return an 'argument' structure.
        arg = m.arg_dict.copy()
        for i,v in enumerate(m.arg_list):
            arg['Argument%d' % (i+1)] = v
        return arg
    
    def message_value(self, m):
        '''
        Given Message m, return value from m.arg_list/m.arg_dict.
        If self.param == _ALL_, return {m.arg_name: value}.
        '''
        value = self.raw_message_value(m)
        if self.param == "_ALL_":
            return {m.arg_name: value}
        else:
            return value
    
    def value(self):
        '''
        Return newest parameter value from self.messages.
        No messages are removed from the queue.
        TODO: Timeout + wait if no messages?  Currently will throw.
        '''
        return self.message_value(self.messages.peek_newest())
    
    def pop(self):
        '''
        Pop oldest message and return the parameter value.
        This destroys the oldest message; only the value is returned.
        TODO: Timeout + wait if no messages?  Currently will throw.
        '''
        return self.message_value(self.messages.pop())


class Parameter(Transaction):
    '''
    Gets or sets SDP parameters in remote tasks,
    depending on whether 'value' is supplied to __init__.
    
    Examples:
        # simple synchronous get's
        x = Parameter("TASK", "POS.X").value()  # no timeout
        y = Parameter("TASK", "POS.Y").value(5) # 5s timeout
        
        # simple synchronous set's
        Parameter("TASK", "POS.X", 42.0).join()  # no timeout
        Parameter("TASK", "POS.Y", 53.0).join(5) # 5s timeout
        
        # asynchronous, multi-param get (TODO function for this)
        params = ['X', 'Y', 'Z']
        params = {p:Parameter("TASK", p) for p in params}
        while any([p.running for p in params.values()]): wait()  # TODO func
        params = {k:v.value() for k,v in params.items()}
        x = params['X']
        y = params['Y']
        z = params['Z']
    
    Instance attributes (besides those from Transaction):
        task       str, name of target task
        param      str, name of target SDP parameter
        type       str, 'get' or 'set'
        _value     obj, from completion msg once value() called
    
    TODO: Can we get/set multiple parameters with a single transaction?
    
    NOTE: Setting parameters only seems to work for single values;
          you cannot set entire structs or arrays.
          The value you set will be coerced to the existing type;
          you will get an error for invalid string->number conversions.
          Setting struct array items like 's[1].value' will succeed,
          but always raises SDS__NOITEM, which is terrible.
          I cannot figure out how to set data array values at all.
    '''
    
    def __init__(self, task, param, value=None):
        '''
        If value is None (the default), calls DitsGetParam, else DitsSetParam.
        
        WARNING, path resolution may call wait(10), which allows
        other actions to run and can accept messages and raise exceptions
        for ANY of your action's running Transactions.
        '''
        cdef StatusType status = 0
        cdef DitsTransIdType transid
        cdef DitsPathType path
        self.task = task
        self.param = param
        path = get_path(task)
        if value is None:
            self.type = 'get'
            func = 'DitsGetParam(%s,%s)' % (task, param)
            DitsGetParam(path, param, &transid, &status)
        else:
            self.type = 'set'
            func = 'DitsSetParam(%s,%s,%s)' % (task, param, value)
            #argid = make_argument(value)
            argid = sds_from_obj(value)
            DitsSetParam(path, param, argid, &transid, &status)
            delete_sds(argid)
        if status:
            raise BadStatus(status, func)
        Transaction.__init__(self, int(<ulong>transid))
    
    def __repr__(self):
        return 'Parameter(%s %s.%s)' % (self.type, self.task, self.param)
    
    def message_value(self, m):
        '''
        Given Message m, returns value from m.arg_list/m.arg_dict.
        This function relies on self.param, so make sure to
        only use it with this instance's own messages!
        '''
        if m is None:  # maybe if _ALL_ is empty?
            return {}
        # check for the expected case, arg_dict = {param:value}
        if len(m.arg_dict) == 1 and len(m.arg_list) == 0 \
            and (m.arg_dict.keys()[0] == self.param \
                or self.param.endswith('.%s' % (m.arg_dict.keys()[0]))
                or (self.param == '_NAMES_' and m.arg_dict.keys()[0] == 'Names')):
            return m.arg_dict.values()[0]
        # otherwise could be weird, return an 'argument' structure.
        arg = m.arg_dict.copy()
        for i,v in enumerate(m.arg_list):
            arg['Argument%d' % (i+1)] = v
        return arg
        
    def value(self, timeout=None):
        '''
        Waits up to timeout secs (or forever if None, the default)
        for self.running to be False, then returns the value taken
        from the completion message argument or raises Timeout.
        
        timeout can be absolute (seconds since 19700101) ala time.time().
        
        Caches the value as self._value for repeated calls.
        
        BEWARE: This function calls wait(), which allows other actions to run
        and can receive messages and raise exceptions for ANY of your action's
        running Transaction objects (Signal/Obey/Kick/Monitor/etc).
        '''
        if hasattr(self, '_value'):
            return self._value
        self.join(timeout)
        m = None
        while self.messages:
            m = self.messages.pop()
        if self.type == 'set':
            self._value = None
        else:
            self._value = self.message_value(m)
        return self._value


def trigger(*args, **kwargs):
    '''
    Construct a Dits argument from *args/**kwargs and call
    DitsTrigger to send a message to the parent action.
    '''
    cdef StatusType status = 0
    argid = make_argument(*args, **kwargs)
    DitsTrigger(argid, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsTrigger(%d)" % (argid) )


def signal(action, *args, **kwargs):
    '''
    Construct a Dits argument from *args/**kwargs and call
    DitsSignalByName to send a message to another action in this task.
    '''
    cdef StatusType status = 0
    argid = make_argument(*args, **kwargs)
    DitsSignalByName(action, argid, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsSignalByName(%s,%d)" % (action, argid) )


def blind_kick(task, action, *args, **kwargs):
    '''
    Construct a Dits argument from *args/**kwargs and call
    DitsKick on task.action using a NULL transid.
    Use it when:
     - you did not start the action yourself (use Obey.kick() instead)
     - you don't care if the target action is running (no MESREJECTED)
     - you don't care when the target action completes (no COMPLETE)
     - you are planning on sending numerous kicks to a long-running
       action as part of a custom communication scheme and you don't
       want to leak a bunch of transactions (since kick transactions
       stick around until the target task completes).

    Use the Kick class instead if you want to keep track of your kick.
    
    WARNING, path resolution may call wait(10), which allows
        other actions to run and can accept messages and raise exceptions
        for ANY of your action's running Transactions.
    '''
    cdef StatusType status = 0
    cdef DitsPathType path
    path = get_path(task)
    argid = make_argument(*args, **kwargs)
    DitsKick(path, action, argid, NULL, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsKick(%s,%s,%d)" % (task, action, argid) )


def blind_obey(task, action, *args, **kwargs):
    '''
    Construct a Dits argument from *args/**kwargs and call
    DitsObey on task.action using a NULL transid.
    Use it when:
     - you don't care about success, failure, completion
     - you need to start a local action before entering the main loop
    
    Use the Obey class instead if you want to keep track of your obey.
    
    WARNING, path resolution may call wait(10), which allows
        other actions to run and can accept messages and raise exceptions
        for ANY of your action's running Transactions.
    '''
    cdef StatusType status = 0
    cdef DitsPathType path
    path = get_path(task)
    argid = make_argument(*args, **kwargs)
    DitsObey(path, action, argid, NULL, &status)
    delete_sds(argid)
    if status != 0:
        raise BadStatus(status, "DitsObey(%s,%s,%d)" % (task, action, argid) )


def msgout(m):
    '''
    Calls MsgOut(STATUS__OK, str).
    Sends a DITS_MSG_MESSAGE to initiator of the current action (immediately).
    Also copies message to JIT_MSG_OUT parameter for external monitors.
    '''
    cdef StatusType status = 0
    m = str(m)
    MsgOut(&status, m)
    if status != 0:
        raise BadStatus(status, "MsgOut(%s)" % (m) )
    # emulate jit_MsgOut, if task initialized
    if _fd != -1:
        set_param("JIT_MSG_OUT", m)


def ersrep(e):
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
    cdef StatusType status = 0
    e = str(e)
    flags = ERS_M_NOFMT | ERS_M_HIGHLIGHT
    ErsRep(flags, &status, e)
    if status != 0:
        raise BadStatus(status, "ErsRep(%s)" % (e) )
    # I'm not sure if this is correct --
    #   maybe ersrep needs to allow multiple messages to queue up?
    #   maybe MESSAGE *has* to be 200 chars long?
    if _fd != -1:
        set_param("JIT_ERS_OUT", {'TASKNAME': _taskname,
                                  'MESSAGE': [e],
                                  'FLAGS': [_numpy.int32(flags)],
                                  'STATUS': [_numpy.int32(0)]} )


def ersout(e):
    '''
    Calls ErsOut(ERS_M_NOFMT | ERS_M_HIGHLIGHT, STATUS__OK, str).
    Also copies message to JIT_ERS_OUT parameter (TODO is this correct?)
    ErsOut is equivalent to ErsRep + ErsFlush.
    Reports an error to initiator of the current action (immediately).
    
    TODO: Does ERS_M_HIGHLIGHT suffice for red output in JOSCON,
          or do we need to set bad status as well?
    '''
    cdef StatusType status = 0
    e = str(e)
    flags = ERS_M_NOFMT | ERS_M_HIGHLIGHT
    ErsOut(flags, &status, e)
    if status != 0:
        raise BadStatus(status, "ErsOut(%s)" % (e) )
    # TODO is this correct?
    if _fd != -1:
        set_param("JIT_ERS_OUT", {'TASKNAME': _taskname,
                                  'MESSAGE': [e],
                                  'FLAGS': [_numpy.int32(flags)],
                                  'STATUS': [_numpy.int32(0)]} )


def _wait(secs):
    '''
    Main interface for yielding control back to DRAMA.
    Sleeps up to 'secs' seconds (or forever if None) until a message arrives.
    'secs' can also be an absolute timeout, seconds since 19700101.
    Updates and returns the associated Transaction object.
    Raises:
        Timeout    on RESCHED
        Kicked     on KICK
        Died       on DIED  (TODO if transid 0x0 is parent, raise Died('TASK').
        BadStatus  on MESREJECTED
        BadStatus  on COMPLETE w/bad status
        Unexpected on TRIGGER if no monitor/obey handler.
        Unexpected if no transaction found
        Unexpected on other reasons
    '''
    cdef StatusType status = 0
    g = _greenlet.getcurrent()
    transactions = g.transactions

    if secs is None:
        DitsPutRequest(DITS_REQ_SLEEP, &status)
    else:
        s = float(secs)
        if s > 315360000.0:  # 10*365*86400
            s = s - time.time()
        if s <= 0.0:
            raise Timeout(secs)
        jitDelayRequest(s, &status)

    # return control to DRAMA and wait for next message
    _log.debug("_wait: switching from greenlet %s to parent %s" % (g, g.parent))
    msg = g.parent.switch()
    _log.debug("_wait: switch returned %s" % (msg))

    try:
        if msg.reason == DITS_REA_RESCHED:
            raise Timeout(secs)
        elif msg.reason == DITS_REA_KICK:
            raise Kicked(msg)
        elif msg.reason == DITS_REA_ASTINT:
            # signal from peer, get Signal instance to pass this to
            obj = transactions[0]
            obj.messages.push(msg)
        elif msg.reason == DITS_REA_COMPLETE \
             or msg.reason == DITS_REA_DIED \
             or msg.reason == DITS_REA_MESREJECTED \
             or msg.reason == DITS_REA_PATHFOUND \
             or msg.reason == DITS_REA_PATHFAILED:
            # find the matching transaction handler
            obj = transactions[msg.transid]
            obj.running = False
            obj.status = msg.status
            # might be a parameter message, push if args
            if msg.arg_list or msg.arg_dict:
                obj.messages.push(msg)
            del transactions[msg.transid]  # transaction complete
            if msg.reason == DITS_REA_DIED:
                raise Died(obj)
            if msg.reason == DITS_REA_PATHFAILED:
                msg.status = msg.status or DITS__INVPATH
            if msg.status != 0:
                raise BadStatus(msg.status, obj)
        elif msg.reason == DITS_REA_TRIGGER:
            # find the matching transaction handler
            obj = transactions[msg.transid]
            if msg.status == DITS__MON_STARTED:
                obj.monid = int(msg.arg_dict['MONITOR_ID'])
                obj.running = True
            elif msg.status == DITS__MON_CHANGED or isinstance(obj, Obey):
                obj.messages.push(msg)
            else:
                raise Unexpected(msg)
        else:
            raise Unexpected(msg)
    except KeyError:  # failed to find an object for this message
        raise Unexpected(msg)
    return obj


def delay(secs):
    '''
    Wait 'secs' seconds for a RESCHED, regardless of any other messages
    that arrive in the meantime.  Use this instead of time.sleep()
    to maintain concurrency with other actions.
    'secs' can be an absolute timeout, seconds since 19700101.
    '''
    until = absolute_timeout(float(secs))  # cannot be None
    try:
        _wait(secs)  # try to avoid instant timeout for tiny secs
        while True:
            _wait(until)  # now use the absolute timeout
    except Timeout:
        pass


def wait(secs=None, objs=None):
    '''
    Wait up to 'secs' (or forever if None) for messages affecting one of
    'objs' (or any if None) and return the affected object.
    'secs' can be an absolute timeout, seconds since 19700101.
    Note that this function can raise exceptions for any transactions
    started by the current action, not just those in 'objs'.
    Raises:
        Timeout    on RESCHED
        Kicked     on KICK
        Died       on DIED
        BadStatus  on MESREJECTED
        BadStatus  on COMPLETE w/bad status
        Unexpected on TRIGGER if no monitor/obey handler.
        Unexpected if no transaction found
        Unexpected on other reasons
    '''
    if objs is None or objs is False or objs is 0:
        objs = []
    elif not hasattr(objs, '__iter__'):
        objs = [objs]
    for o in objs:
        if not isinstance(o, Transaction):
            raise TypeError('bad wait type ' + str(type(o)))
    
    until = absolute_timeout(secs)
    try:
        obj = _wait(secs)  # try to avoid instant timeout for tiny secs
        while objs and obj not in objs:
            obj = _wait(until)  # now use the absolute timeout
    except Timeout:
        raise Timeout(secs, objs)
    return obj


cdef void dispatcher(StatusType *status):
    '''C entry point for all registered DRAMA actions.'''
    cdef StatusType tstatus = 0
    
    # make sure dispatcher is never called from an action context
    global _dispatcher_greenlet
    if _dispatcher_greenlet is None:
        _dispatcher_greenlet = _greenlet.getcurrent()
    elif _dispatcher_greenlet != _greenlet.getcurrent():
        status[0] = DITS__APP_ERROR
        #DitsPutRequest(DITS_REQ_EXIT, &tstatus)  # doesn't work
        DitsPutRequest(DITS_REQ_END, &tstatus)
        blind_obey(_taskname, "EXIT")
        act = 'unknown'
        for n,v in _greenlets.iteritems():
            if v == _greenlet.getcurrent():
                act = n
                break
        _log.critical("dispatcher called from %s action greenlet, %s" % \
                      (act, _greenlet.getcurrent()))
        return
    
    # bad entry status or failing to get entry details is a FATAL error
    n = None  # action name (msg.name), used frequently
    try:
        msg = Message()  # grabs Dits entry info
        _log.debug("dispatcher entry message: %s" % (msg))
        n = msg.name
    except (TypeError, ValueError):
        status[0] = DITS__INVARG
        _log.exception('dispatcher: invalid arg getting entry details')
    except BadStatus as e:
        status[0] = e.status or DITS__APP_ERROR
        _log.exception('dispatcher: bad status getting entry details: %s' % (e))
    except:
        status[0] = DITS__APP_ERROR
        _log.exception('dispatcher: error getting entry details')
    finally:
        if status[0] != 0:
            if n is not None:
                bs = BadStatus(status[0], '%s: bad status on entry' % (n))
                _log.critical('%s' % (bs))
            #DitsPutRequest(DITS_REQ_EXIT, &tstatus)  # doesn't work
            DitsPutRequest(DITS_REQ_END, &tstatus)
            blind_obey(_taskname, "EXIT")
            return
    
    # create new or reenter old greenlet for action; handle errors and cleanup.
    try:
        if msg.reason == DITS_REA_OBEY:
            g = _greenlet.greenlet(_actions[n])
            _greenlets[n] = g
            g.transactions = {}
            _log.debug("dispatcher switching to new %s greenlet %s" % (n, g))
            g.switch(*msg.arg_list, **msg.arg_dict)
        else:
            g =  _greenlets[n]
            _log.debug("dispatcher switching to %s greenlet %s" % (n, g))
            g.switch(msg)
    except (Kicked, Died, Unexpected):
        status[0] = DITS__UNEXPMSG
        _log.exception('%s: unexpected entry reason' % (n))
    except Timeout:
        status[0] = DITS__APP_TIMEOUT
        _log.exception('%s: timeout' % (n))
    except (TypeError, ValueError):
        status[0] = DITS__INVARG
        _log.exception('%s: invalid argument' % (n))
    except BadStatus as e:
        status[0] = e.status or DITS__APP_ERROR
        _log.exception('%s: bad status' % (n))
    except Exit as e:
        _log.debug('%s: %s' % (n, repr(e)))
        #DitsPutRequest(DITS_REQ_EXIT, &tstatus)  # doesn't work
        DitsPutRequest(DITS_REQ_END, &tstatus)
        blind_obey(_taskname, "EXIT")
        return
    except:
        status[0] = DITS__APP_ERROR
        _log.exception('%s: other error' % (n))
    finally:
        g = _greenlets.get(n, None)
        if status[0] != 0 or (g is not None and g.dead):
            DitsPutRequest(DITS_REQ_END, &tstatus)
            if g is not None:
                for m in g.transactions.values():
                    if isinstance(m, Monitor):
                        try:
                            m.cancel()
                        except:
                            _log.exception('%s: error canceling %s' % (n, m))
                _log.debug("dispatcher: destroying %s greenlet %s" % (n, g))
                g.throw()  # raises GreenletExit to kill the greenlet
                del _greenlets[n]
                del g.transactions
                del g


cdef void orphan_handler(StatusType *status):
    '''C orphan transaction handler, cleans up stray monitors.'''
    cdef DitsGsokMessageType message
    cdef DitsPathType path
    msg = Message()  # grabs Dits entry info
    _log.debug('orphan_handler entry message: %s' % (msg))
    # TODO spit out an ErsRep or something?  where would it go?
    if msg.reason == DITS_REA_TRIGGER and msg.status == DITS__MON_STARTED:
        monid = int(msg.arg_dict['MONITOR_ID'])
        _log.debug('orphan_handler: canceling monitor %s:%d' % \
                   (msg.task, monid))
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
          flags = DITS_M_X_COMPATIBLE | DITS_M_IMB_ROUND_ROBIN,
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
    
    # make sure global environment is cleaned up
    stop(taskname)
    
    # flags must include X_COMPATIBLE for select() loop to work
    flags |= DITS_M_X_COMPATIBLE
    _log.debug('init: flags: %d = 0x%x' % (flags, flags))
    
    #jitSetDefaults( flags, 0.0, *buffers, &status )
    b = buffers
    _log.debug('init: buffer sizes: %s' % (b))
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
    global _fd, _taskname, _dispatcher_greenlet
    _fd = -2
    _taskname = taskname
    _dispatcher_greenlet = None

    DitsPutOrphanHandler(orphan_handler, &status)
    if status != 0:
        raise BadStatus(status, "DitsPutOrphanHandler")
    
    # register global _actions{} using the C dispatcher() function
    if actions:
        for action in actions:
            register_action(action.__name__, action)
            
    if tidefile is not None:
        _log.debug('init: tideInit(%s)' % (tidefile))
        tideInit(&_altin, tidefile, &status)
        if status != 0:
            raise BadStatus(status, "tideInit(%s)" % (tidefile) )
        _altin.exit_flag = 0
        _log.debug('init: saved altin: 0x%lx' % (<ulong>_altin))
    
    acts = 'init: %s actions:' % (taskname)
    for k,v in _actions.items():
        acts += '\n  %s : %s' % (k,v)
    _log.debug(acts)


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
    '''
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
        for i in xrange(DITS_C_ALT_IN_MAX):
            if _altin.Array[i].number < 0:
                continue
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

    if fd < 0:
        # TODO raise exception?
        return

    # Is the main Dits fd?
    if fd == _fd:
        while not exit_flag and DitsMsgAvail(&status):
            DitsMsgReceive(&exit_flag, &status)
        if status:
            raise BadStatus(status, 'DitsMsgReceive')
        if exit_flag:
            raise Exit('DitsMsgReceive')
        return
        
    # Is it a user-callback registered fd?
    if fd in _callbacks:
        _callbacks[fd](fd)
        return

    # Is it a TIDE fd?
    if _altin:
        for i in xrange(DITS_C_ALT_IN_MAX):
            if fd == _altin.Array[i].number:
                _altin.Array[i].routine(_altin.Array[i].client_data, &status)
                if status or _altin.exit_flag:
                    s = 'TIDE routine, fd %d, index %d' % (fd, i)
                    if _altin.exit_flag:
                        if status:
                            raise BadStatus(status, s)
                        raise Exit(s)
                    # ditsaltin.c ignores non-exit status errors
                    bs = BadStatus(status, s)
                    _log.warn('%s' % (bs))
                    ErsClear(&status)


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
    an update() method as 'tk', which might be useful if you want something
    called periodically and don't want to set up an action for it.
    '''
    
    # Tkinter is a big module, don't import unnecessarily
    if 'Tkinter' in _sys.modules:
        _log.debug('run: using Tkinter')
        import Tkinter as _Tkinter
        TclError = _Tkinter.TclError
        if tk is None:
            tk = _Tkinter._default_root
    else:
        TclError = None  # 'except None as e' is okay
    
    timeout_seconds = None
    if tk is not None:
        timeout_seconds = 1.0/hz
        _log.debug('run: will call %s.update() every %g seconds' % \
                          (repr(tk), timeout_seconds) )
        
    try:
        while True:
            # fd's might change, must check every time
            r,w,x = get_fd_sets()
            sr,sw,sx = [],[],[]
            try:
                sr,sw,sx = _select.select(r,w,x, timeout_seconds)
            except _select.error as e:
                # we can ignore 'Interrupted system call'
                if e.args[0] != _errno.EINTR:
                    raise
            fds = set(sr) | set(sw) | set(sx)
            for fd in fds:
                process_fd(fd)
            if tk is not None:
                tk.update()
    except Exit:
        # catch Exit() so it doesn't cause bad $? exit status
        pass
    except TclError as e:
        # exit quietly if tk destroyed, else reraise
        if e.args[0].find('application has been destroyed') < 0:
            raise


def stop(taskname=None):
    '''
    If called from inside an action, raises Exit.
    
    Otherwise, cleans up DRAMA globals and calls jitStop(taskname).
    If taskname==None, use the global _taskname from init().
    
    You MUST call this function to make sure the task is unregistered
    from the IMP network system; a finally: block is a good place for it.
    '''
    cdef StatusType status = 0
    global _taskname, _altin, _fd, _greenlets, _callbacks, _actions
    
    if _greenlet.getcurrent() in _greenlets.values():
        raise Exit('stop')
    
    # Make sure all greenlets are dead so they don't continue
    # to hold refs to user resources.  We also cancel any
    # outstanding Monitor instances before jitStop().
    for n,g in _greenlets.items():
        _log.debug('stop: canceling monitors in %s' % (n))
        for m in g.transactions.values():
            if isinstance(m, Monitor):
                try:
                    m.cancel()
                except:
                    e = _sys.exc_info()[1]
                    _log.warn('stop: error canceling %s: %s' % (m, repr(e)))
        _log.debug('stop: killing greenlet %s' %  (n))
        g.throw()  # raises GreenletExit
    
    # Must clean up _altin manually; tideExit() is not enough.
    if _altin:
        _log.debug('stop: calling tideExit()')
        tideExit(&status)
        DitsFree(_altin)
        _altin = NULL
        if status != 0:
            bs =  BadStatus(status, "tideExit")
            _log.warn('%s' % (bs))
            status = 0

    # Use _fd as a guard to avoid jitStop() segfaults.
    if _fd != -1:
        if taskname is None:
            taskname = _taskname
        _log.debug('stop: calling jitStop(%s)' % (taskname))
        jitStop(taskname, &status)
        _fd = -1
        if status != 0:
            bs = BadStatus(status, "jitStop(%s)" % (taskname) )
            _log.warn('%s' % (bs))
    
    # Clean up remaining globals.
    _greenlets = {}
    _callbacks = {}
    _actions = {}


