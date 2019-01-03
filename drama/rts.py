'''
Framework for 'monitor' RTS clients.

Author: Ryan Berthold, EAO

Port of the DRAMA RTS Client framework to Python.
User supplies callbacks that are invoked by actions defined here:

    def initialise(msg)
    def configure(msg, wait_set, done_set)
    def setup_sequence(msg, wait_set, done_set)
    def sequence(msg)
    def sequence_frame(frame)
    def sequence_batch(state)

To wait for other tasks to finish CONFIGURE or SETUP_SEQUENCE
(that is, to set their CONFIGURE_ID or SETUP_SEQ_ID to a given value),
the configure() and setup_sequence() callbacks take additional parameters.

The user can place any tasknames they wish to wait for in the wait_set;
as the tasks complete, their tasknames are copied to the done_set.
The action is automatically rescheduled while there are outstanding tasks.

You might not wait on certain tasks while in simulate or engineering mode;
in that case the following pattern might prove helpful:

    if msg.reason == drama.REA_OBEY:
        if not (simulate or engineering):
            wait_set.add(task)
    
    if task in wait_set and task in done_set:
        wait_set.remove(task)
        # real work, once only
    elif task not in wait_set and task not in done_set:
        done_set.add(task)
        # fake work, once only
    else:
        # still waiting or already complete

This pattern is also useful when there are multiple tasks to wait on,
so the callback can handle each task (once) as soon as it completes.

NOTE: To prevent infinite loops, the done_set is effectively add-only.
Any items you remove from it will be added back by the parent action
when your callback returns, and will be included for the next call.

TODO: Provide functions to simplify the pattern above?
My main worry is that they would make things more confusing,
since it's not necessarily obvious that the function
is modifying done_set/wait_set and will only trigger once.

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


def handle_sets(msg, wait_set, done_set, transid_dict, param, matchval, badval):
    '''
    Monitor tasks, waiting for them to set param to matchval.
    If any task sets its param to badval, raise an exception.
      msg: current drama.Message instance for this action
      wait_set: given tasks to wait on
      done_set: modified to remember finished tasks
      transid_dict: modified to remember [transid]:[taskname,monid]
      param: given parameter name to monitor
      matchval: given value to wait for
      badval: given value to choke on
    
    You can check completion using issubset or the equivalent set notation:
      if not (wait_set <= done_set):
        drama.reschedule()
    '''
    new_tasks = wait_set - set(x[0] for x in transid_dict.values())
    for t in new_tasks:
        _log.debug('handle_sets creating monitor on task %s', t)
        transid_dict[_drama.monitor(t, param)] = [t,None]
    if msg.reason == _drama.REA_TRIGGER and msg.transid in transid_dict:
        _log.debug('handle_sets msg: %s', msg)
        if msg.status == _drama.MON_STARTED:
            transid_dict[msg.transid][1] = msg.arg['MONITOR_ID']
        elif msg.status == _drama.MON_CHANGED:
            v = msg.arg[param]  # TODO msg.arg form?
            if v in (matchval, badval):
                t,m = transid_dict[msg.transid]
                _drama.cancel(t,m)
                done_set.add(t)
            if v == badval:
                raise _drama.BadStatus(RTSDC__GERROR, 'Task %s bad %s' % (t,param))
            
            


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
    
    TODO: ought to read in the INITIALISE xml too, if present.
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
            # cache task sets for future calls
            CONFIGURE.wait_set = set()
            CONFIGURE.done_set = set()
            CONFIGURE.transid_dict = {}
            CONFIGURE.CONFIGURE_ID = CONFIGURE_ID
        
        ws = CONFIGURE.wait_set
        ds = CONFIGURE.done_set
        td = CONFIGURE.transid_dict
        cid = CONFIGURE.CONFIGURE_ID
        handle_sets(msg, ws, ds, td, 'CONFIGURE_ID', cid, -9999)
        
        global configure
        if configure is not None:
            _log.debug("CONFIGURE: calling user callback.")
            # done_set is add-only to prevent infinite reschedules
            dsc = ds.copy()
            ret = configure(msg, ws, dsc)
            ds.update(dsc)
        else:
            _log.debug("CONFIGURE: no user callback.")
        
        if _drama.rescheduled():
            return ret
        
        if not (ws <= ds):  # not done waiting
            _drama.reschedule()
            return ret
        
        _log.debug("CONFIGURE: setting CONFIGURE_ID=%d" % (cid))
        _drama.set_param("CONFIGURE_ID", cid)
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
            # cache task sets for future calls
            SETUP_SEQUENCE.wait_set = set()
            SETUP_SEQUENCE.done_set = set()
            SETUP_SEQUENCE.transid_dict = {}
            SETUP_SEQUENCE.SETUP_SEQ_ID = SETUP_SEQ_ID
        
        ws = SETUP_SEQUENCE.wait_set
        ds = SETUP_SEQUENCE.done_set
        td = SETUP_SEQUENCE.transid_dict
        ssid = SETUP_SEQUENCE.SETUP_SEQ_ID
        handle_sets(msg, ws, ds, td, 'SETUP_SEQ_ID', ssid, -9999)
        
        global setup_sequence
        if setup_sequence is not None:
            _log.debug("SETUP_SEQUENCE: calling user callback.")
            # done_set is add-only to prevent infinite reschedules
            dsc = ds.copy()
            ret = setup_sequence(msg, ws, dsc)
            ds.update(dsc)
        else:
            _log.debug("SETUP_SEQUENCE: no user callback.")
        
        if _drama.rescheduled():
            return ret
        
        if not (ws <= ds):  # not done waiting
            _drama.reschedule()
            return ret
        
        _log.debug("SETUP_SEQUENCE: setting SETUP_SEQ_ID=%d" % (ssid))
        _drama.set_param("SETUP_SEQ_ID", ssid)
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
                        # TODO: check continuity.
                        # should NUMBER come from rts_state instead of local counter?
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
            _drama.reschedule(False)  # cancels user reschedule too
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
    

