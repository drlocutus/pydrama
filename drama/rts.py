'''
Framework for 'monitor' RTS clients.

Author: Ryan Berthold, JAC
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


# TODO: Need defs for these, from rtsDClient_err.h:
#  RTSDC__ACTION_WHILE_SEQ_ACTIVE
#  RTSDC__NOT_INITIALISED
#  RTSDC__NOT_CONFIGURED
#  RTSDC__NOT_SETUP


def task_wait(funcname, monparam, matchval, badval=-9999,
              wait_tasks=[], path_tasks=[]):
    '''
    Helper function to monitor a group of tasks and wait until
    their values match an expected value.
        funcname: Calling function for logging purposes.
        monparam: Parameter name to monitor in remote tasks.
        matchval: Expected value of monitored parameter.
        badval: Failure value of monitored parameter; raises Kicked.
        wait_tasks: Wait for these tasks' to match CONFIGURE_ID.
        path_tasks: Cache DRAMA paths to these tasks.
    '''
    _log.debug("%s: %s=%s, !=%s, wait_tasks=%s, path_tasks=%s" % \
               (funcname, monparam, matchval, badval, wait_tasks, path_tasks))
    monitors = []
    try:
        for t in path_tasks:
            _drama.cache_path(t)
        for t in wait_tasks:
            monitors.append(_drama.Monitor(t, monparam))
        while monitors:
            m = _drama.wait(objs=monitors)
            if m.messages:
                v = m.value()  # or should we pop raw messages?
                if v == matchval:  # target task done
                    _log.debug("%s: %s done." % (funcname, m.task))
                    m.cancel()
                elif v == badval:  # target task had configure trouble; kickme
                    _log.error("%s: %s failed." % (funcname, m.task))
                    raise _drama.Kicked(m.messages.peek_newest())
                m.messages.clear()
            if not m.running:
                monitors.remove(m)
    finally:
        for m in monitors:
            m.messages.clear()
            m.cancel()
    _log.debug("%s: done." % (funcname))


def configure_wait(CONFIGURE_ID, wait_tasks=[], path_tasks=[]):
    '''
    Call this from your CONFIGURE callback if you need to
    wait for other tasks to finish their CONFIGURE actions.
    Supply two lists of task names:
        wait_tasks: Wait for these tasks' to match CONFIGURE_ID.
        path_tasks: Cache DRAMA paths to these tasks.
    '''
    task_wait("configure_wait",
              "CONFIGURE_ID", CONFIGURE_ID, -9999, wait_tasks, path_tasks)


def setup_sequence_wait(SETUP_SEQ_ID, wait_tasks=[], path_tasks=[]):
    '''
    Call this from your SETUP_SEQUENCE callback if you need to
    wait for other tasks to finish their SETUP_SEQUENCE actions.
    Supply two lists of task names:
        wait_tasks: Wait for these tasks' SETUP_SEQ_ID to match mine.
        path_tasks: Cache DRAMA paths to these tasks.
    Any wait_tasks not in the TASKS list are moved to path_tasks.
    '''
    tasks = _drama.get_param("TASKS").split()
    for t in wait_tasks[:]:  # iterate over a copy -- remove() paranoia
        if not t in tasks:
            wait_tasks.remove(t)
            path_tasks.append(t)
    task_wait("setup_sequence_wait",
              "SETUP_SEQ_ID", SETUP_SEQ_ID, -9999, wait_tasks, path_tasks)


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
    
    Notes on user callbacks:
        - They are invoked with the same *args/**kwargs as their actions.
        - They should reraise any exceptions they catch so the actions
          can do proper cleanup.
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


def INITIALISE(*args, **kwargs):
    '''
    Reset progress parameters, get state spool params (STSPL_*),
    invoke user initialise callback, then set INITIALISED=1.
    '''
    _log.debug("INITIALISE: setting params, checking args.")
    _drama.set_param("INITIALISED", 0)
    _drama.set_param("CONFIGURED", 0)
    _drama.set_param("SETUP", 0)
    _drama.set_param("IN_SEQUENCE", 0)
    _drama.set_param("SIMULATE", kwargs.get("SIMULATE", 32767))  # ???
    _drama.set_param("STSPL_TOTAL", kwargs.get("STSPL_TOTAL", 1))
    _drama.set_param("STSPL_START", kwargs.get("STSPL_START", 0))
    
    global initialise
    if initialise is not None:
        _log.debug("INITIALISE: calling user callback.")
        ret = initialise(*args, **kwargs)
    else:
        _log.debug("INITIALISE: no user callback.")
    
    _log.debug("INITIALISE: setting INITIALISED=1.")
    _drama.set_param("INITIALISED", 1)
    
    _log.debug("INITIALISE: done.")
    return ret


def CONFIGURE(CONFIGURATION="", CONFIGURE_ID=1, ENGIN_MODE=0, *args, **kwargs):
    '''
    Load up the CONFIGURATION xml file, invoke user callback,
    and announce our completion by setting CONFIGURE_ID.
    '''
    try:
        _log.debug("CONFIGURE: checking progress params.")
        if not _drama.get_param("INITIALISED"):
            raise _drama.BadStatus(RTSDC__NOT_INITIALISED,
                                   "CONFIGURE: not yet initialised")
        if _drama.get_param("IN_SEQUENCE"):
            raise _drama.BadStatus(RTSDC__ACTION_WHILE_SEQ_ACTIVE,
                                   "CONFIGURE: sequence still active")
        
        _log.debug("CONFIGURE: setting params.")
        _drama.set_param("CONFIGURED", 0)
        _drama.set_param("CONFIGURE_ID", -1)
        _drama.set_param("ENGIN_MODE", ENGIN_MODE)
        
        if CONFIGURATION:
            _drama.set_param("CONFIGURATION",
                             _drama.obj_from_xml(CONFIGURATION))
        
        global configure
        if configure is not None:
            _log.debug("CONFIGURE: calling user callback.")
            ret = configure(CONFIGURATION=CONFIGURATION,
                            CONFIGURE_ID=CONFIGURE_ID,
                            ENGIN_MODE=ENGIN_MODE, *args, **kwargs)
        else:
            _log.debug("CONFIGURE: no user callback.")
        
        _log.debug("CONFIGURE: setting CONFIGURE_ID=%d" % (CONFIGURE_ID))
        _drama.set_param("CONFIGURE_ID", CONFIGURE_ID)
        _drama.set_param("CONFIGURED", 1)
        
        _log.debug("CONFIGURE: done.")
        return ret
        
    except:
        _drama.set_param("CONFIGURE_ID", -9999)
        raise
        

def SETUP_SEQUENCE(SETUP_SEQ_ID=1, *args, **kwargs):
    '''
    Get args, invoke user callback, publish SETUP_SEQ_ID.
    There are many kwargs available that we don't bother with;
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
    try:
        _log.debug("SETUP_SEQUENCE: checking progress params.")
        if not _drama.get_param("INITIALISED"):
            raise _drama.BadStatus(RTSDC__NOT_INITIALISED,
                                   "SETUP_SEQUENCE: not yet initialised")
        if not _drama.get_param("CONFIGURED"):
            raise _drama.BadStatus(RTSDC__NOT_CONFIGURED,
                                   "SETUP_SEQUENCE: not yet configured")
        if _drama.get_param("IN_SEQUENCE"):
            raise _drama.BadStatus(RTSDC__ACTION_WHILE_SEQ_ACTIVE,
                                   "SETUP_SEQUENCE: sequence still active")
        
        _log.debug("SETUP_SEQUENCE: setting params.")
        _drama.set_param("SETUP", 0)
        _drama.set_param("SETUP_SEQ_ID", -1)
        _drama.set_param("TASKS", kwargs.get("TASKS", "").upper())
        
        global setup_sequence
        if setup_sequence is not None:
            _log.debug("SETUP_SEQUENCE: calling user callback.")
            ret = setup_sequence(SETUP_SEQ_ID=SETUP_SEQ_ID, *args, **kwargs)
        else:
            _log.debug("SETUP_SEQUENCE: no user callback.")
        
        _log.debug("SETUP_SEQUENCE: setting SETUP_SEQ_ID=%d" % (SETUP_SEQ_ID))
        _drama.set_param("SETUP_SEQ_ID", SETUP_SEQ_ID)
        _drama.set_param("SETUP", 1)
    
        _log.debug("SETUP_SEQUENCE: done.")
        return ret
        
    except:
        _drama.set_param("SETUP_SEQ_ID", -9999)
        raise


def SEQUENCE(START=1, END=2, DWELL=1, *args, **kwargs):
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
    try:
        _log.debug("SEQUENCE: checking progress params.")
        if not _drama.get_param("INITIALISED"):
            raise _drama.BadStatus(RTSDC__NOT_INITIALISED,
                                   "SEQUENCE: not yet initialised")
        if not _drama.get_param("CONFIGURED"):
            raise _drama.BadStatus(RTSDC__NOT_CONFIGURED,
                                   "SEQUENCE: not yet configured")
        if not _drama.get_param("SETUP"):
            raise _drama.BadStatus(RTSDC__NOT_SETUP,
                                   "SEQUENCE: not yet setup")
        
        _log.debug("SEQUENCE: setting params.")
        _drama.set_param("IN_SEQUENCE", 0)  # no check?
        _drama.set_param("START", START)
        _drama.set_param("END", END)
        _drama.set_param("DWELL", DWELL)
        _drama.set_param("STSPL_BUFFCOUNT", 0)
        _drama.set_param("STSPL_INDEX", 1)
        stspl_total = _drama.get_param("STSPL_TOTAL")
        stspl_start = _drama.get_param("STSPL_START")
        stspl_publish = START + stspl_total + stspl_start - 1
        if stspl_publish > END:
            stspl_publish = END
        _drama.set_param("STSPL_PUBLISH", stspl_publish)
        
        global sequence
        if sequence is not None:
            _log.debug("SEQUENCE: calling user callback.")
            sequence(START=START, END=END, DWELL=DWELL, *args, **kwargs)
        else:
            _log.debug("SEQUENCE: no user callback.")
        
    except:
        # I'm not sure why we do this even on error
        _drama.set_param("SEQUENCE_ID", START)
        raise
    
    rts = None
    try:
        _log.debug("SEQUENCE: starting RTS.STATE monitor.")
        _drama.set_param("IN_SEQUENCE", 1)
        rts = Monitor(REAL_TIME_SEQ_TASK, "STATE")
        while not rts.messages:
            rts.wait()
        rts.messages.clear()  # ignore current state
    except:
        if rts is not None:
            rts.cancel()
        raise
    finally:
        # Again, always set this regardless of errors
        _drama.set_param("SEQUENCE_ID", START)
    
    try:
        i = START - 1
        state = []
        _log.debug("SEQUENCE: starting monitor loop.")
        while rts.running:
            rts.wait()
            while rts.messages:
                rts_state = rts.pop()
                for rts_frame in rts_state:
                    # TODO: check continuity
                    i += 1
                    if i == END:
                        rts.cancel()
                    frame = {"NUMBER": i}
                    if sequence_frame:
                        frame = sequence_frame(frame) or frame
                    state.append(frame)
                    if i == stspl_publish:
                        stspl_publish += stspl_total
                        if stspl_publish > END:
                            stspl_publish = END
                        if sequence_batch:
                            state = sequence_batch(state) or state
                        _drama.set_param("STATE", state)
                        state = []
                        _drama.set_param("STSPL_INDEX", 1)
                        _drama.set_param("STSPL_PUBLISH", stspl_publish)
                        _drama.set_param("STSPL_BUFFCOUNT",
                                         _drama.get_param("STSPL_BUFFCOUNT")+1)
                    else:
                        _drama.set_param("STSPL_INDEX",
                                         _drama.get_param("STSPL_INDEX")+1)
        _log.debug("SEQUENCE: monitor loop done.")
    finally:
        rts.cancel()
        rts.messages.clear()
        _drama.set_param("IN_SEQUENCE", 0)
        _drama.set_param("SEQUENCE_ID", -1)

