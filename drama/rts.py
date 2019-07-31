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

--------

Regarding KICKS: On kick, the main priority is to make sure the action ends
instead of waiting forever.  The user callbacks will be called ONCE with
REA_KICK, then the parent action will raise BadStatus to cancel any
user reschedules.  You can still use wait() in your kick handler if necessary,
but remember that any outstanding monitors will be canceled automatically
when the action ends -- don't make extra work for yourself.

'''


import drama as _drama
import logging as _logging
import numpy  # for int32


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
globals().update(rtsDClient_err_d)
# error number to name lookup -- yagni?
rtsDClient_err_n = {}
for name,number in rtsDClient_err_d.items():
    rtsDClient_err_n[number] = name


# hack, todo read from somewhere legit
REAL_TIME_SEQ_TASK = 'RTS'


class TaskWaiter(object):
    '''
    Used by CONFIGURE and SETUP_SEQUENCE to wait for a set of tasks to finish.
    They create an instance of this class on OBEY, attach the instance to the
    function, and use the same instance on subsequent calls.
    '''
    def __init__(self, param, matchval, badval):
        '''
        param: Parameter to monitor in each task, CONFIGURE_ID or SETUP_SEQ_ID.
        matchval: The value indicating success in the monitored tasks.
        badval:   The value indicating failure in the monitored tasks.
        '''
        self.param = param
        self.matchval = matchval
        self.badval = badval
        self.transid_dict = {}  # [transid]:[taskname,monid]
        self.wait_set = set()
        self.done_set = set()  # only ADD to this set!
        _log.debug('TaskWaiter(%s, %s, %s)', param, matchval, badval)
    
    def start_monitors(self):
        '''
        Start monitors on any new tasks in self.wait_set.
        Called after user callback has (maybe) modified self.wait_set.
        '''
        new_tasks = self.wait_set - set(x[0] for x in self.transid_dict.values())
        valid_tasks = _drama.get_param("TASKS").split()
        for task in new_tasks:
            if valid_tasks and task not in valid_tasks:
                _log.debug('TaskWaiter skipping %s, not in TASKS list', task)
                self.done_set.add(task)
            else:
                _log.debug('TaskWaiter starting monitor %s:%s', task, self.param)
                self.transid_dict[_drama.monitor(task, self.param)] = [task,None]
    
    def check_monitors(self, msg):
        '''
        Check msg for task completion and add to done_set.
        Called before user callback.
        Raise an exception if any task sets its param to badval.
        '''
        if msg.reason == _drama.REA_TRIGGER and msg.transid in self.transid_dict:
            _log.debug('TaskWaiter got msg %s', msg)
            if msg.status == _drama.MON_STARTED:
                self.transid_dict[msg.transid][1] = msg.arg['MONITOR_ID']
            elif msg.status == _drama.MON_CHANGED:
                val = msg.arg
                if val in (self.matchval, self.badval):
                    task,mid = self.transid_dict[msg.transid]
                    _drama.cancel(task,mid)
                    self.done_set.add(task)
                if val == self.badval:
                    raise _drama.BadStatus(RTSDC__GERROR, 'Task %s bad %s %d' % (task, self.param, val))
    
    def waiting(self):
        '''
        Return True if done_set does not contain all tasks in wait_set.
        '''
        return not (self.wait_set <= self.done_set)


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
    _drama.set_param("SIMULATE", numpy.int32(0))
    _drama.set_param("CONFIGURE_ID", numpy.int32(-1))
    _drama.set_param("SETUP_SEQ_ID", numpy.int32(-1))
    _drama.set_param("SEQUENCE_ID", numpy.int32(-1))
    _drama.set_param("ENGIN_MODE", numpy.int32(0))
    _drama.set_param("RTSDC_FLAGS", numpy.int32(0))  # unused
    _drama.set_param("INITIALISED", numpy.int32(0))
    _drama.set_param("CONFIGURED", numpy.int32(0))
    _drama.set_param("SETUP", numpy.int32(0))
    _drama.set_param("IN_SEQUENCE", numpy.int32(0))
    # these two params are not actually used or needed, but set to match fesim
    _drama.set_param("CONFIG_KICKED", numpy.int32(0))
    _drama.set_param("SETSEQ_KICKED", numpy.int32(0))
    # not sure what the next four params are for, but set to match fesim
    _drama.set_param("SKIP_SR_HIGH", numpy.int32(0))
    _drama.set_param("SKIP_SR_LOW", numpy.int32(0))
    _drama.set_param("QUICK_START", numpy.int32(0))
    _drama.set_param("QUICK_END", numpy.int32(0))
    # SEQUENCE publishing cadence
    _drama.set_param("STSPL_INDEX", numpy.int32(0))
    _drama.set_param("STSPL_TOTAL", numpy.int32(0))
    _drama.set_param("STSPL_START", numpy.int32(0))
    _drama.set_param("STSPL_PUBLISH", numpy.int32(0))
    _drama.set_param("STSPL_BUFFCOUNT", numpy.int32(0))
    # SETUP_SEQUENCE args
    _drama.set_param("SOURCE", "")
    _drama.set_param("INDEX", numpy.int32(0))
    _drama.set_param("POL_INDEX", numpy.int32(0))
    _drama.set_param("INDEX1", numpy.int32(0))
    _drama.set_param("MS_INDEX", numpy.int32(0))
    _drama.set_param("GROUP", numpy.int32(0))
    _drama.set_param("DRCONTROL", numpy.int32(0))
    _drama.set_param("BEAM", "")
    _drama.set_param("STEP_TIME", 0.0)
    _drama.set_param("SMU_X", 0.0)
    _drama.set_param("SMU_Y", 0.0)
    _drama.set_param("SMU_Z", 0.0)
    _drama.set_param("LOAD", "")
    _drama.set_param("FE_STATE", "")
    _drama.set_param("TASKS", "")
    _drama.set_param("MASTER", "")
    _drama.set_param("BB_TEMP", 0.0)
    _drama.set_param("SHUT_FRAC", 0.0)
    _drama.set_param("HEAT_CUR", numpy.int32(0))
    # STATE structure array for SEQUENCE
    _drama.set_param("STATE", [{"NUMBER":numpy.int32(0)}])
    
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
    if msg.reason == _drama.REA_OBEY:
        INITIALISE.ret = None
        _log.debug("INITIALISE: setting params, checking args.")
        _drama.set_param("INITIALISED", numpy.int32(0))
        _drama.set_param("CONFIGURED", numpy.int32(0))
        _drama.set_param("SETUP", numpy.int32(0))
        _drama.set_param("IN_SEQUENCE", numpy.int32(0))
        _drama.set_param("SIMULATE", numpy.int32(msg.arg.get("SIMULATE", 32767)))  # bitmask
        _drama.set_param("STSPL_TOTAL", numpy.int32(msg.arg.get("STSPL_TOTAL", 1)))
        _drama.set_param("STSPL_START", numpy.int32(msg.arg.get("STSPL_START", 0)))
    
    global initialise
    if initialise is not None:
        _log.debug("INITIALISE: calling user callback.")
        ret = initialise(msg)
        if ret is not None:
            INITIALISE.ret = ret
    else:
        _log.debug("INITIALISE: no user callback.")
    
    if msg.reason == _drama.REA_KICK:
        # TODO there ought to be a better status code for kicks
        raise _drama.BadStatus(RTSDC__GERROR, "INITIALISE kicked, ending action")
    
    if _drama.rescheduled():
        return
    
    _log.debug("INITIALISE: setting INITIALISED=1.")
    _drama.set_param("INITIALISED", numpy.int32(1))
    
    _log.debug("INITIALISE: done.")
    
    # return cached value, removing our static reference
    ret = INITIALISE.ret
    del INITIALISE.ret
    return ret


def CONFIGURE_ARGS(CONFIGURATION="", CONFIGURE_ID=1, ENGIN_MODE=0, *args, **kwargs):
    CONFIGURE_ID = int(CONFIGURE_ID)
    ENGIN_MODE = int(ENGIN_MODE)
    return CONFIGURATION, CONFIGURE_ID, ENGIN_MODE
    

def CONFIGURE(msg):
    '''
    Load up the CONFIGURATION xml file, invoke user callback,
    and announce our completion by setting CONFIGURE_ID.
    '''
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
            CONFIGURE.ret = None
            _log.debug("CONFIGURE: setting params.")
            _drama.set_param("CONFIGURED", numpy.int32(0))
            _drama.set_param("CONFIGURE_ID", numpy.int32(-1))
            _drama.set_param("ENGIN_MODE", numpy.int32(ENGIN_MODE))
            _drama.set_param("TASKS", "")  # clear TASKS so TaskWaiter ignores
            if CONFIGURATION:
                _drama.set_param("CONFIGURATION",
                                 _drama.obj_from_xml(CONFIGURATION))
            # cache TaskWaiter instance for future calls
            CONFIGURE.tw = TaskWaiter('CONFIGURE_ID', CONFIGURE_ID, -9999)

        tw = CONFIGURE.tw
        tw.check_monitors(msg)
        
        global configure
        if configure is not None:
            _log.debug("CONFIGURE: calling user callback.")
            dsc = tw.done_set.copy()  # add-only, prevents infinite reschedules
            ret = configure(msg, tw.wait_set, dsc)
            if ret is not None:
                CONFIGURE.ret = ret
            tw.done_set.update(dsc)
        else:
            _log.debug("CONFIGURE: no user callback.")
        
        if msg.reason == _drama.REA_KICK:
            raise _drama.BadStatus(RTSDC__GERROR, "CONFIGURE kicked, ending action")
        
        _log.debug("CONFIGURE: wait_set: %s, done_set: %s", tw.wait_set, tw.done_set)
        tw.start_monitors()
        if _drama.rescheduled():  # user rescheduled already
            return
        elif tw.waiting():  # outstanding tasks in wait_set
            _drama.reschedule()
            return
        
        _log.debug("CONFIGURE: setting CONFIGURE_ID=%d" % (tw.matchval))
        _drama.set_param("CONFIGURE_ID", numpy.int32(tw.matchval))
        _drama.set_param("CONFIGURED", numpy.int32(1))
        _log.debug("CONFIGURE: done.")
        
        # return cached value, removing our static reference
        ret = CONFIGURE.ret
        del CONFIGURE.ret
        return ret
        
    except:
        _drama.set_param("CONFIGURE_ID", numpy.int32(-9999))
        raise
        

def SETUP_SEQUENCE_ARGS(SETUP_SEQ_ID=1, *args, **kwargs):
    '''
    NOTE: Not all params may be present; preserve current Sdp values. 
    TODO: check valid ranges and set defaults for Sdp parameters:
    
        SOURCE: if does not start with REFERENCE, must start with SCIENCE.
        BEAM: [A, B, MIDDLE]
        LOAD: [SKY, LOAD2, AMBIENT, LINE, DARK, HOT]
        FE_STATE: defl OFFSETZERO
        TASKS:
        MASTER:
        
        INDEX: [0,32766]
        POL_INDEX: [0,32766]
        INDEX1: [0,32766]
        MS_INDEX: [0,32766]
        GROUP: [0,32766]
        DRCONTROL: [0,32766]
        HEAT_CUR: [-99999, 131071]
        
        STEP_TIME: [0.004, 600.0], defl 0.5
        SMU_X: [-35.0, 35.0]
        SMU_Y: [-35.0, 35.0]
        SMU_Z: [-35.0, 35.0]
        BB_TEMP: [-99999.0, 80.0] defl 10.0
        SHUT_FRAC: [0.0, 1.0], defl 0.0
        
    '''
    SETUP_SEQ_ID = int(SETUP_SEQ_ID)
    # not all params may be present; preserve current Sdp values.
    # updated in groups of desired Sds type.
    for k in ['SOURCE', 'BEAM', 'LOAD', 'FE_STATE', 'TASKS', 'MASTER']:
        if k in kwargs:
            _drama.set_param(k, kwargs[k])  # strings
    for k in ['INDEX', 'POL_INDEX', 'INDEX1', 'MS_INDEX', 'GROUP', 'DRCONTROL', 'HEAT_CUR']:
        if k in kwargs:
            _drama.set_param(k, numpy.int32(kwargs[k]))
    for k in ['STEP_TIME', 'SMU_X', 'SMU_Y', 'SMU_Z', 'BB_TEMP', 'SHUT_FRAC']:
        if k in kwargs:
            _drama.set_param(k, float(kwargs[k]))
    
    return SETUP_SEQ_ID
    

def SETUP_SEQUENCE(msg):
    '''
    Get args, invoke user callback, publish SETUP_SEQ_ID.
    '''
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
            SETUP_SEQUENCE.ret = None
            _log.debug("SETUP_SEQUENCE: setting params.")
            _drama.set_param("SETUP", numpy.int32(0))
            _drama.set_param("SETUP_SEQ_ID", numpy.int32(-1))
            # TASKS is now handled in SETUP_SEQUENCE_ARGS
            #_drama.set_param("TASKS", msg.arg.get("TASKS", ""))  # valid for TaskWaiter
            # cache TaskWaiter instance for future calls
            SETUP_SEQUENCE.tw = TaskWaiter('SETUP_SEQ_ID', SETUP_SEQ_ID, -9999)
        
        tw = SETUP_SEQUENCE.tw
        tw.check_monitors(msg)
        
        global setup_sequence
        if setup_sequence is not None:
            _log.debug("SETUP_SEQUENCE: calling user callback.")
            dsc = tw.done_set.copy()  # add-only, prevents infinite reschedules
            ret = setup_sequence(msg, tw.wait_set, dsc)
            if ret is not None:
                SETUP_SEQUENCE.ret = ret
            tw.done_set.update(dsc)
        else:
            _log.debug("SETUP_SEQUENCE: no user callback.")
        
        if msg.reason == _drama.REA_KICK:
            raise _drama.BadStatus(RTSDC__GERROR, "SETUP_SEQUENCE kicked, ending action")
        
        _log.debug("SETUP_SEQUENCE: wait_set: %s, done_set: %s", tw.wait_set, tw.done_set)
        tw.start_monitors()
        if _drama.rescheduled():  # user rescheduled already
            return
        elif tw.waiting():  # outstanding tasks in wait_set
            _drama.reschedule()
            return
        
        _log.debug("SETUP_SEQUENCE: setting SETUP_SEQ_ID=%d" % (tw.matchval))
        _drama.set_param("SETUP_SEQ_ID", numpy.int32(tw.matchval))
        _drama.set_param("SETUP", numpy.int32(1))
        _log.debug("SETUP_SEQUENCE: done.")
        
        # return cached value, removing our static reference
        ret = SETUP_SEQUENCE.ret
        del SETUP_SEQUENCE.ret
        return ret
        
    except:
        _drama.set_param("SETUP_SEQ_ID", numpy.int32(-9999))
        raise


def SEQUENCE_ARGS(START=1, END=1, DWELL=1, *args, **kwargs):
    return int(START), int(END), int(DWELL)


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
            SEQUENCE.ret = None
            _log.debug("SEQUENCE: setting params.")
            _drama.set_param("IN_SEQUENCE", numpy.int32(0))
            _drama.set_param("START", numpy.int32(SEQUENCE.start))
            _drama.set_param("END", numpy.int32(SEQUENCE.end))
            _drama.set_param("DWELL", numpy.int32(SEQUENCE.dwell))
            _drama.set_param("STSPL_BUFFCOUNT", numpy.int32(0))
            _drama.set_param("STSPL_INDEX", numpy.int32(1))
            SEQUENCE.stspl_total = _drama.get_param("STSPL_TOTAL")
            SEQUENCE.stspl_start = _drama.get_param("STSPL_START")
            SEQUENCE.stspl_publish = SEQUENCE.start + SEQUENCE.stspl_total + SEQUENCE.stspl_start - 1
            if SEQUENCE.stspl_publish > SEQUENCE.end:
                SEQUENCE.stspl_publish = SEQUENCE.end
            _drama.set_param("STSPL_PUBLISH", numpy.int32(SEQUENCE.stspl_publish))
            _log.debug("SEQUENCE: starting RTS.STATE monitor.")
            SEQUENCE.transid = _drama.monitor(REAL_TIME_SEQ_TASK, "STATE")
            SEQUENCE.state = []
            SEQUENCE.i = SEQUENCE.start - 1
        
        global sequence, sequence_frame, sequence_batch
        if sequence is not None:
            _log.debug("SEQUENCE: calling user callback.")
            ret = sequence(msg)
            if ret is not None:
                SEQUENCE.ret = ret
        else:
            _log.debug("SEQUENCE: no user callback.")
        
        if not _drama.rescheduled():
            _drama.reschedule()
        
        if msg.reason == _drama.REA_TRIGGER and msg.transid == SEQUENCE.transid:
            if msg.status == _drama.MON_STARTED:
                SEQUENCE.monid = msg.arg['MONITOR_ID']
            elif msg.status == _drama.MON_CHANGED:
                # DEBUG: not sure yet what to expect here
                _log.debug("SEQUENCE msg: %s", msg)
                if not _drama.get_param("IN_SEQUENCE"):  # ignore current state
                    _drama.set_param("IN_SEQUENCE", numpy.int32(1))
                    _drama.set_param("SEQUENCE_ID", numpy.int32(SEQUENCE.start))
                else:  # in sequence
                    rts_state = msg.arg
                    for rts_frame in rts_state:
                        # TODO: check continuity.
                        # should NUMBER come from rts_state instead of local counter?
                        SEQUENCE.i += 1
                        if SEQUENCE.i != rts_frame["NUMBER"]:
                            raise _drama.BadStatus(RTSDC__GERROR,
                                "NUMBER mismatch, mine=%d, rts=%d"%(SEQUENCE.i, rts_frame["NUMBER"]))
                        if SEQUENCE.i == SEQUENCE.end:
                            _drama.cancel(REAL_TIME_SEQ_TASK, SEQUENCE.monid)
                        frame = {"NUMBER":numpy.int32(SEQUENCE.i)}  # TODO include other params from RTS?
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
                            _drama.set_param("STSPL_INDEX", numpy.int32(1))
                            _drama.set_param("STSPL_PUBLISH", numpy.int32(SEQUENCE.stspl_publish))
                            _drama.set_param("STSPL_BUFFCOUNT",
                                             numpy.int32(_drama.get_param("STSPL_BUFFCOUNT")+1))
                        else:
                            _drama.set_param("STSPL_INDEX",
                                             numpy.int32(_drama.get_param("STSPL_INDEX")+1))
                            
        if msg.reason == _drama.REA_COMPLETE and msg.transid == SEQUENCE.transid:
            _log.debug("SEQUENCE: done.")
            _drama.reschedule(False)  # cancels user reschedule too
            _drama.set_param("IN_SEQUENCE", numpy.int32(0))
            _drama.set_param("SEQUENCE_ID", numpy.int32(-1))
            
            # return cached value, removing our static reference
            ret = SEQUENCE.ret
            del SEQUENCE.ret
            return ret
        
        if msg.reason == _drama.REA_KICK:
            raise _drama.BadStatus(RTSDC__GERROR, "SEQUENCE kicked, ending action")
        
        # TODO: handle unexpected entry reasons?
        # if user callback is doing something exotic, we can't know.
        # might just have to log it without raising an error.
        
    except:
        # set good SEQUENCE_ID first to wake up the RTS
        _drama.set_param("SEQUENCE_ID", numpy.int32(SEQUENCE.start))
        _drama.set_param("IN_SEQUENCE", numpy.int32(0))
        _drama.set_param("SEQUENCE_ID", numpy.int32(-1))
        raise
    

