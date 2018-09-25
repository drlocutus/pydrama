'''
Framework for 'monitor' RTS clients.

Author: Ryan Berthold, EAO

Example:  TODO
'''


import drama as _drama
import logging as _logging


_log = _logging.getLogger(__name__)
_log.addHandler(_logging.NullHandler())


# User callbacks for main actions
initialise = None
configure = None
setup_sequence = None
sequence = None
sequence_frame = None
sequence_batch = None

# import error codes as global vars, e.g. RTSDC__NOT_INITIALISED
rtsDClient_err_h = '/jac_sw/itsroot/install/rts/include/rtsDClient_err.h'
rtsDClient_err_d = _drama.errors_from_header(rtsDClient_err_h)
globals.update(rtsDClient_err_d)
# error number to name lookup -- yagni?
rtsDClient_err_n = {}
for name,number in rtsDClient_err_d.items():
    rtsDClient_err_n[number] = name


# hack, todo read from somewhere legit
REAL_TIME_SEQ_TASK = 'RTS'


def tasks_complete(msg, tasks, param=None, matchval=None, badval=None):
    '''
    Return True if all tasks have set param to matchval, else False.
    If incoming msg sets task.param to badval, raise an exception.
    This function will save 'param', 'matchval', and 'badval' if given;
    normally these are set by the parent action to prime this function,
    so the user callback should ignore them.
    
    NOTE: Since this function caches parameters, you cannot use it to
    wait simultaneously on two different sets of tasks/params.
    '''
    if param is not None:
        _log.debug('tasks_complete reset: %s %s %s', param, matchval, badval)
        tasks_complete.param = param
        tasks_complete.matchval = matchval
        tasks_complete.badval = badval
        tasks_complete.tids = None
        return
    if tasks_complete.tids is None:
        tids = {}  # transid: (taskname, monid)
        for t in tasks:
            tids[_drama.monitor(t, tasks_complete.param)] = [t,None]
        tasks_complete.tids = tids
    if msg.reason == _drama.REA_TRIGGER and msg.transid in tasks_complete.tids:
        if msg.status == _drama.MON_STARTED:
            tasks_complete.tids[msg.transid][1] = msg.arg['MONITOR_ID']
        elif msg.status == _drama.MON_CHANGED:
            _log.debug('tasks_complete msg: %s', msg)
            v = msg.arg[tasks_complete.param]  # TODO msg.arg form?
            if v == tasks_complete.matchval or v == tasks_complete.badval:
                t,m = tasks_complete.tids.pop(msg.transid)
                _drama.cancel(t,m)
            if v == tasks_complete.badval:
                raise _drama.BadStatus(RTSDC__GERROR, 'Task %s bad %s' % (t,tasks_complete.param))
    return (not tasks_complete.tids)  # True if empty


def init(user_initialise_callback=None,
         user_configure_callback=None,
         user_setup_sequence_callback=None,
         user_sequence_callback=None,
         user_sequence_frame_callback=None,
         user_sequence_batch_callback=None):
    '''
    Call this function after drama.init() to set up RTS parameters,
    register INITIALISE/CONFIGURE/SETUP_SEQUENCE/SEQUENCE actions,
    and register user callbacks for those actions.
    '''
    _log.debug("init: setting up parameters.")
    _drama.set_param("CONFIGURE_ID", -1)
    _drama.set_param("SETUP_SEQ_ID", -1)
    _drama.set_param("SEQUENCE_ID", -1)
    _drama.set_param("ENGIN_MODE", 0)
    _drama.set_param("SIMULATE", 0)
    _drama.set_param("INITIALISED", 0)
    _drama.set_param("CONFIGURED", 0)
    _drama.set_param("SETUP", 0)
    _drama.set_param("IN_SEQUENCE", 0)
    _drama.set_param("STSPL_INDEX", 0)
    _drama.set_param("STSPL_TOTAL", 0)
    _drama.set_param("STSPL_START", 0)
    _drama.set_param("STSPL_PUBLISH", 0)
    _drama.set_param("STSPL_BUFFCOUNT", 0)
    _drama.set_param("TASKS", "")
    _drama.set_param("STATE", [{"NUMBER":0}])  # STATE structure array
    
    _log.debug("init: registering actions.")
    _drama.register_action("INITIALISE", INITIALISE)
    _drama.register_action("CONFIGURE", CONFIGURE)
    _drama.register_action("SETUP_SEQUENCE", SETUP_SEQUENCE)
    _drama.register_action("SEQUENCE", SEQUENCE)
    
    _log.debug("init: registering callbacks.")
    global initialise, configure, setup_sequence, sequence
    global sequence_frame, sequence_batch
    initialise = user_initialise_callback
    configure = user_configure_callback
    setup_sequence = user_setup_sequence_callback
    sequence = user_sequence_callback
    sequence_frame = user_sequence_frame_callback
    sequence_batch = user_sequence_batch_callback
    
    _log.debug("init: done.")


def INITIALISE(msg):
    '''
    Reset progress parameters, get state spool params (STSPL_*),
    invoke user initialise callback, then set INITIALISED=1.
    '''
    ret = None
    if msg.reason == _drama.REA_OBEY:
        _log.debug("INITIALISE: setting params, checking args.")
        _drama.set_param("INITIALISED", 0)
        _drama.set_param("CONFIGURED", 0)
        _drama.set_param("SETUP", 0)
        _drama.set_param("IN_SEQUENCE", 0)
        _drama.set_param("SIMULATE", msg.arg.get("SIMULATE", 32767))  # bitmask
        _drama.set_param("STSPL_TOTAL", msg.arg.get("STSPL_TOTAL", 1))
        _drama.set_param("STSPL_START", msg.arg.get("STSPL_START", 0))
    
    global initialise
    if initialise is not None:
        _log.debug("INITIALISE: calling user callback.")
        ret = initialise(msg)
    else:
        _log.debug("INITIALISE: no user callback.")
    
    if _drama.rescheduled():
        return ret
    
    _log.debug("INITIALISE: setting INITIALISED=1.")
    _drama.set_param("INITIALISED", 1)
    
    _log.debug("INITIALISE: done.")
    return ret


def CONFIGURE_ARGS(CONFIGURATION="", CONFIGURE_ID=1, ENGIN_MODE=0, *args, **kwargs):
    return CONFIGURATION, CONFIGURE_ID, ENGIN_MODE
    

def CONFIGURE(msg):
    '''
    Load up the CONFIGURATION xml file, invoke user callback,
    and announce our completion by setting CONFIGURE_ID.
    '''
    ret = None
    try:
        if msg.reason == _drama.REA_OBEY:
            _log.debug("CONFIGURE: checking readiness.")
            if not _drama.get_param("INITIALISED"):
                raise _drama.BadStatus(RTSDC__NOT_INITIALISED,
                                       "CONFIGURE: not yet initialised")
            if _drama.get_param("IN_SEQUENCE"):
                raise _drama.BadStatus(RTSDC__ACTION_WHILE_SEQ_ACTIVE,
                                       "CONFIGURE: sequence still active")
            args,kwargs = _drama.parse_argument(msg.arg)
            CONFIGURATION, CONFIGURE_ID, ENGIN_MODE = CONFIGURE_ARGS(*args,**kwargs)
            _log.debug("CONFIGURE: setting params.")
            _drama.set_param("CONFIGURED", 0)
            _drama.set_param("CONFIGURE_ID", -1)
            _drama.set_param("ENGIN_MODE", ENGIN_MODE)
            if CONFIGURATION:
                _drama.set_param("CONFIGURATION",
                                 _drama.obj_from_xml(CONFIGURATION))
            # cache CONFIGURE_ID for future calls
            tasks_complete(msg,[], 'CONFIGURE_ID', CONFIGURE_ID, -9999)
        
        global configure
        if configure is not None:
            _log.debug("CONFIGURE: calling user callback.")
            ret = configure(msg)
        else:
            _log.debug("CONFIGURE: no user callback.")
        
        if _drama.rescheduled():
            return ret
        
        CONFIGURE_ID = tasks_complete.matchval  # retrieve cached value
        _log.debug("CONFIGURE: setting CONFIGURE_ID=%d" % (CONFIGURE_ID))
        _drama.set_param("CONFIGURE_ID", CONFIGURE_ID)
        _drama.set_param("CONFIGURED", 1)
        
        _log.debug("CONFIGURE: done.")
        return ret
        
    except:
        _drama.set_param("CONFIGURE_ID", -9999)
        raise
        

def SETUP_SEQUENCE_ARGS(SETUP_SEQ_ID=1, *args, **kwargs):
    return SETUP_SEQ_ID
    

def SETUP_SEQUENCE(msg):
    '''
    Get args, invoke user callback, publish SETUP_SEQ_ID.
    There are many kwargs available that we don't bother with here;
    get them yourself in your user callback if you need them:
        SOURCE: if does not start with REFERENCE, must start with SCIENCE.
        INDEX: [0,32766]
        POL_INDEX: [0,32766]
        INDEX1: [0,32766]
        MS_INDEX: [0,32766]
        GROUP: [0,32766]
        DRCONTROL: [0,32766]
        BEAM: [A, B, MIDDLE]
        SMU_X: [-35.0, 35.0]
        SMU_Y: [-35.0, 35.0]
        SMU_Z: [-35.0, 35.0]
        LOAD: [SKY, LOAD2, AMBIENT, LINE, DARK, HOT]
        FE_STATE: defl OFFSETZERO
        STEP_TIME: [0.004, 600.0], defl 0.5
        MASTER
        BB_TEMP: [-99999.0, 80.0] defl 10.0
        SHUT_FRAC: [0.0, 1.0], defl 0.0
        HEAT_CUR: [-99999, 131071]
    '''
    ret = None
    try:
        if msg.reason == _drama.REA_OBEY:
            _log.debug("SETUP_SEQUENCE: checking readiness.")
            if not _drama.get_param("INITIALISED"):
                raise _drama.BadStatus(RTSDC__NOT_INITIALISED,
                                       "SETUP_SEQUENCE: not yet initialised")
            if not _drama.get_param("CONFIGURED"):
                raise _drama.BadStatus(RTSDC__NOT_CONFIGURED,
                                       "SETUP_SEQUENCE: not yet configured")
            if _drama.get_param("IN_SEQUENCE"):
                raise _drama.BadStatus(RTSDC__ACTION_WHILE_SEQ_ACTIVE,
                                       "SETUP_SEQUENCE: sequence still active")
            args,kwargs = _drama.parse_argument(msg.arg)
            SETUP_SEQ_ID = SETUP_SEQUENCE_ARGS(*args,**kwargs)
            _log.debug("SETUP_SEQUENCE: setting params.")
            _drama.set_param("SETUP", 0)
            _drama.set_param("SETUP_SEQ_ID", -1)
            _drama.set_param("TASKS", msg.arg.get("TASKS", "").upper())
            # cache SETUP_SEQ_ID for future calls
            tasks_complete(msg,[], 'SETUP_SEQ_ID', SETUP_SEQ_ID, -9999)
        
        global setup_sequence
        if setup_sequence is not None:
            _log.debug("SETUP_SEQUENCE: calling user callback.")
            ret = setup_sequence(msg)
        else:
            _log.debug("SETUP_SEQUENCE: no user callback.")
        
        if _drama.rescheduled():
            return ret
        
        SETUP_SEQ_ID = tasks_complete.matchval  # retrieve cached value
        _log.debug("SETUP_SEQUENCE: setting SETUP_SEQ_ID=%d" % (SETUP_SEQ_ID))
        _drama.set_param("SETUP_SEQ_ID", SETUP_SEQ_ID)
        _drama.set_param("SETUP", 1)
    
        _log.debug("SETUP_SEQUENCE: done.")
        return ret
        
    except:
        _drama.set_param("SETUP_SEQ_ID", -9999)
        raise


def SEQUENCE_ARGS(START=1, END=2, DWELL=1, *args, **kwargs):
    return START, END, DWELL


def SEQUENCE(msg):
    '''
    Does the following:
        - Check progress params
        - Set sequence params
        - Invoke user sequence callback
        - Start RTS.STATE monitor
        - Call sequence_frame for each frame in RTS.STATE;
            modify frame in-place or return a new one.
        - Call sequence_batch before publishing a batch of frames;
            modify array in-place or return a new one.
        - Once done, clear IN_SEQUENCE and SEQUENCE_ID
    '''
    ret = None
    try:
        if msg.reason == _drama.REA_OBEY:
            _log.debug("SEQUENCE: checking readiness.")
            if not _drama.get_param("INITIALISED"):
                raise _drama.BadStatus(RTSDC__NOT_INITIALISED,
                                       "SEQUENCE: not yet initialised")
            if not _drama.get_param("CONFIGURED"):
                raise _drama.BadStatus(RTSDC__NOT_CONFIGURED,
                                       "SEQUENCE: not yet configured")
            if not _drama.get_param("SETUP"):
                raise _drama.BadStatus(RTSDC__NOT_SETUP,
                                       "SEQUENCE: not yet setup")
            args,kwargs = _drama.parse_argument(msg.arg)
            SEQUENCE.start, SEQUENCE.end, SEQUENCE.dwell = SEQUENCE_ARGS(*args,**kwargs)
            _log.debug("SEQUENCE: setting params.")
            _drama.set_param("IN_SEQUENCE", 0)
            _drama.set_param("START", SEQUENCE.start)
            _drama.set_param("END", SEQUENCE.end)
            _drama.set_param("DWELL", SEQUENCE.dwell)
            _drama.set_param("STSPL_BUFFCOUNT", 0)
            _drama.set_param("STSPL_INDEX", 1)
            SEQUENCE.stspl_total = _drama.get_param("STSPL_TOTAL")
            SEQUENCE.stspl_start = _drama.get_param("STSPL_START")
            SEQUENCE.stspl_publish = SEQUENCE.start + SEQUENCE.stspl_total + SEQUENCE.stspl_start - 1
            if SEQUENCE.stspl_publish > SEQUENCE.end:
                SEQUENCE.stspl_publish = SEQUENCE.end
            _drama.set_param("STSPL_PUBLISH", SEQUENCE.stspl_publish)
            _log.debug("SEQUENCE: starting RTS.STATE monitor.")
            SEQUENCE.transid = _drama.monitor(REAL_TIME_SEQ_TASK, "STATE")
            SEQUENCE.state = []
            SEQUENCE.i = SEQUENCE.start - 1
        
        global sequence, sequence_frame, sequence_batch
        if sequence is not None:
            _log.debug("SEQUENCE: calling user callback.")
            ret = sequence(msg)
        else:
            _log.debug("SEQUENCE: no user callback.")
        
        if not _drama.rescheduled():
            _drama.reschedule()
        
        if msg.reason == _drama.REA_TRIGGER and msg.transid == SEQUENCE.transid:
            if msg.status == _drama.MON_STARTED:
                SEQUENCE.monid = msg.arg['MONITOR_ID']
            elif msg.status == _drama.MON_CHANGED:
                if not _drama.get_param("IN_SEQUENCE"):  # ignore current state
                    _drama.set_param("IN_SEQUENCE", 1)
                    _drama.set_param("SEQUENCE_ID", SEQUENCE.start)
                else:  # in sequence
                    # DEBUG: not sure yet what to expect here
                    _log.debug("SEQUENCE msg: %s", msg)
                    rts_state = msg.arg['STATE']
                    for rts_frame in rts_state:
                        # TODO: check continuity
                        SEQUENCE.i += 1
                        if SEQUENCE.i == SEQUENCE.end:
                            _drama.cancel(REAL_TIME_SEQ_TASK, SEQUENCE.monid)
                        frame = {"NUMBER":SEQUENCE.i}
                        if sequence_frame:
                            frame = sequence_frame(frame) or frame
                        SEQUENCE.state.append(frame)
                        if SEQUENCE.i == SEQUENCE.stspl_publish:
                            SEQUENCE.stspl_publish += SEQUENCE.stspl_total
                            if SEQUENCE.stspl_publish > SEQUENCE.end:
                                SEQUENCE.stspl_publish = SEQUENCE.end
                            if sequence_batch:
                                SEQUENCE.state = sequence_batch(SEQUENCE.state) or SEQUENCE.state
                            _drama.set_param("STATE", SEQUENCE.state)
                            SEQUENCE.state = []
                            _drama.set_param("STSPL_INDEX", 1)
                            _drama.set_param("STSPL_PUBLISH", SEQUENCE.stspl_publish)
                            _drama.set_param("STSPL_BUFFCOUNT",
                                             _drama.get_param("STSPL_BUFFCOUNT")+1)
                        else:
                            _drama.set_param("STSPL_INDEX",
                                             _drama.get_param("STSPL_INDEX")+1)
                            
        if msg.reason == _drama.REA_COMPLETE and msg.transid == SEQUENCE.transid:
            _log.debug("SEQUENCE: done.")
            _drama.reschedule(False)
            _drama.set_param("IN_SEQUENCE", 0)
            _drama.set_param("SEQUENCE_ID", -1)
        
        if msg.reason == drama.REA_KICK:
            # bail out and let the auto-cancel handle the monitor, if running.
            _drama.reschedule(False)
            raise _drama.BadStatus(RTSDC__GERROR, 'Kicked during SEQUENCE')
        
        # TODO: handle unexpected entry reasons?
        # if user callback is doing something exotic, we can't know.
        # might just have to log it without raising an error.
        
        return ret
        
    except:
        # set good SEQUENCE_ID first to wake up the RTS
        _drama.set_param("SEQUENCE_ID", SEQUENCE.start)
        _drama.set_param("IN_SEQUENCE", 0)
        _drama.set_param("SEQUENCE_ID", -1)
        raise
    

