#!/local/python3/bin/python3
'''
test/example.py     RMB 20211110


Copyright (C) 2021 East Asian Observatory

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

import sys
import os
import time

# The jac_sw module modifies sys.path so we can find the drama module
# in /jac_sw/itsroot/install/pydrama/lib/Linux-x86_64/python3.7/
import jac_sw
import drama

# SDS object conversion uses numpy for numeric arrays.
# We can also use numpy to force specific types for our data fields.
import numpy

# The name of this task.
# Task names at JCMT are typically capitalized and should be <= 16 chars long. 
taskname = 'PYEX_' + str(os.getpid())

# Set up logging handlers.  If drama.log.setup() is given this taskname
# as a parameter, it will also create a StrftimeHandler that writes
# rolling log files to /jac_logs/<YYYYMMDD>/taskname.log.
import drama.log
drama.log.setup()  # pass taskname if you want to write files to /jac_logs
import logging
log = logging.getLogger(taskname)  # create a named logger for this task
#logging.getLogger('drama').setLevel(logging.DEBUG)

# Global data structure, updated and published by the PUB action.
# Field names should be <= 16 chars long.
pub_data = {
    'timestamp': time.time(),
    'number': 0,
    'mylist': [1,2,'a','b'],
    'mynums': [1.2, 2.3, 3.4, 4.5],
    'myarr': numpy.array([10.0, 20.0, 30.0]),
    'myfloat': numpy.float32(numpy.pi),
    'mydict': {'x':1, 'y':2, 'z':3},
    'mystring': 'hello'
    }

# NOTE: Actions all have NAME(msg) function signatures,
# where msg is an instance of the drama.Message class.
# It can be inspected to determine why the function
# was called and with what arguments.

def PUB(msg):
    '''
    PUB action.
    Reschedules every second to update and publish
    the global pub_data structure as DATA.
    '''
    global pub_data
    pub_data['timestamp'] = time.time()
    pub_data['number'] += 1
    
    # The set_param function converts the given value to an SDS structure,
    # and by default calls SdpUpdate() to put the id in the task's
    # SDP parameter system.  This will allow other tasks to get() the value
    # and also publish the value for any active monitors.
    drama.set_param('DATA', pub_data)
    
    # The reschedule function will cause this action to be called again
    # once the given timeout (in seconds) elapses.
    # Timeouts > 10 years are treated as absolute (wall-clock) times.
    drama.reschedule(1)
    #drama.reschedule(time.time() + 1)  # equivalent
    
    # PUB end


def GET_ARGS(task='', param='DATA', timeout=None):
    '''
    Process and return arguments for GET_S and GET_A actions.
    '''
    task = task or taskname
    timeout = float(timeout) if timeout is not None else None
    return task, param, timeout


def GET_S(msg):
    '''
    GET_S action.
    Performs a synchronous drama.get() on given task.param.
    Since this action calls wait() instead of rescheduling to wait for a reply,
    it assumes that msg.reason is always REA_OBEY and does not check.
    Arguments:
        - task:  target task, default this task (global taskname)
        - param: target parameter name, default "DATA"
        - timeout: timeout in seconds, default None (wait forever)
    '''
    # Extract positional (Argument<n>) and keyword arguments from OBEY message.
    args, kwargs = drama.parse_argument(msg.arg)
    
    # Use a separate function to take advantage of python argument parsing.
    task, param, timeout = GET_ARGS(*args, **kwargs)
    log.info('GET_S(%s, %s, timeout=%s)', task, param, timeout)
    
    # The TransId.wait() function uses DitsActionTransIdWait, which enters
    # a message loop and can call other actions while we wait for a reply.
    # Take care if using wait() in multiple actions,
    # since the first wait() will be stuck until all the nested actions return.
    reply = drama.get(task, param).wait(timeout)
    log.info('GET_S reply: %s', reply)  # Dump full Message
    log.info('GET_S %s: %s', param, reply.arg[param])  # arg is {param:value}
    
    # GET_S end


def GET_A(msg):
    '''
    GET_A action.
    Performs an asynchronous drama.get() on given task.param,
    using a drama.reschedule() to wait for the reply.
    This function will be called again with msg.reason = REA_COMPLETE
    when the reply arrives.
    Arguments:
        - task:  target task, default this task (global taskname)
        - param: target parameter name, default "DATA"
        - timeout: timeout in seconds, default None (wait forever)
    '''
    if msg.reason == drama.REA_OBEY:  # start of action
        # Same argument parsing as GET_S action
        args, kwargs = drama.parse_argument(msg.arg)
        task, param, timeout = GET_ARGS(*args, **kwargs)
        log.info('GET_A(%s, %s, timeout=%s)', task, param, timeout)
        # Save arguments -- local vars will be lost
        # when the function is called again by the reply
        GET_A.task = task
        GET_A.param = param
        GET_A.timeout = timeout
        # Start the get transaction and reschedule to await reply
        drama.get(task, param)
        drama.reschedule(timeout)
    elif msg.reason == drama.REA_COMPLETE:
        log.info('GET_A reply: %s', msg)  # Dump full Message
        log.info('GET_A %s: %s', GET_A.param, msg.arg[GET_A.param])
    elif msg.reason == drama.REA_RESCHED:
        log.error('GET_A timeout after %g seconds', GET_A.timeout)
    else:
        # Being called for any other reason is an error;
        # we do not reschedule and just let the action end.
        log.error('GET_A unexpected msg: %s', msg)
    
    # GET_A end


def MON(msg):
    '''
    MON action.
    Starts monitoring given task.param and logs updates as they arrive.
    Kick this action to cancel the monitor.
    NOTE: This action keeps track of the transaction and monitor IDs,
          explicitly cancels the monitor when needed, and waits for completion.
          But actually the drama module will do all this for us,
          and implicitly cancel the monitor when the action ends,
          if we would rather be lazy.
    Arguments:
        - task:  target task, default this task (global taskname)
        - param: target parameter name, default "DATA"
        - timeout: timeout in seconds, default None (wait forever)
    '''
    if msg.reason == drama.REA_OBEY:
        # Same argument parsing as GET_S action
        args, kwargs = drama.parse_argument(msg.arg)
        task, param, timeout = GET_ARGS(*args, **kwargs)
        log.info('MON(%s, %s, timeout=%s)', task, param, timeout)
        MON.task = task
        MON.param = param
        MON.timeout = timeout
        # We don't really need to save the TransId here,
        # but we would want it if starting multiple monitors.
        MON.tid = drama.monitor(task, param)
        MON.mid = None  # Monitor ID, needed to cancel established monitor
        drama.reschedule(timeout)
    elif msg.reason == drama.REA_COMPLETE:
        # Done, no reschedule
        MON.mid = None
        log.info('MON complete.')
    elif msg.reason == drama.REA_TRIGGER:
        if msg.transid != MON.tid:
            # Wrong monitor.  Pretty weird, but ignore, don't cancel
            log.info('MON unexpected TransId')
        elif msg.status == drama.MON_STARTED:
            MON.mid = msg.arg['MONITOR_ID']
            log.info('MON started, id=%s', MON.mid)  # save for cancel
        elif msg.status == drama.MON_CHANGED:
            log.info('MON changed: %s', msg.arg)
        else:
            # This is too weird to ignore, cancel monitor
            log.error('MON unexpected TRIGGER status: %s', msg)
            drama.cancel(MON.task, MON.mid)
            MON.mid = None
        # reschedule to wait for next message
        drama.reschedule(MON.timeout)
    else:
        # Attempt to cancel but do not reschedule to wait for reply;
        # if a REA_COMPLETE arrives it will go to the Cython orphan_handler().
        log.error('MON unexpected msg: %s', msg)
        drama.cancel(MON.task, MON.mid)
        MON.mid = None
    
    # MON end


# It's important to call drama.stop() to make sure this task's
# communication buffers and semaphores are cleaned up in shared memory.
# Always wrap drama.run() and drama.stop() in a try...finally block.
try:
    log.info('drama.init(%s)', taskname)
    drama.init(taskname, actions=[PUB, GET_S, GET_A, MON])
    
    # Call obey to start the PUB action when we enter the main loop.
    # Outside of an action context we must use blind_obey and blind_kick
    # since we cannot create a transaction id.
    drama.blind_obey(taskname, 'PUB')
    
    # Start the main loop.
    # Task will run until Ctrl-C or EXIT action.
    drama.run()

finally:
    log.info('drama.stop()')
    drama.stop()


'''
# Annotated example output:

[ryanb@koloa pydrama]$ cd test/
[ryanb@koloa test]$ ./example.py &
[1] 25322
[ryanb@koloa test]$ 2021-11-10 12:21:22,983 INFO PYEX_25322: drama.init(PYEX_25322)

# Using ditscmd to send commands to the running task.
# The -gv option will do a verbose "get" on the DATA parameter.

[ryanb@koloa test]$ ditscmd PYEX_25322 -gv DATA
DITSCMD_62ee:ArgStructure         Struct   
DITSCMD_62ee:  DATA                 Struct   
DITSCMD_62ee:    timestamp            Double  1636582910 
DITSCMD_62ee:    number               Ubyte   28 
DITSCMD_62ee:    mylist               Char   [2,4] "1"
DITSCMD_62ee:    mynums               Double [4] { 1.2 2.3 3.4 4.5 }
DITSCMD_62ee:    myarr                Double [3] { 10 20 30 }
DITSCMD_62ee:    myfloat              Float   3.14159 
DITSCMD_62ee:    mydict               Struct   
DITSCMD_62ee:      x                    Ubyte   1 
DITSCMD_62ee:      y                    Ubyte   2 
DITSCMD_62ee:      z                    Ubyte   3 
DITSCMD_62ee:    mystring             Char   [6] "hello"

# Calling the GET_S action with no arguments.
# Note that the log.info() output gets printed twice:
# it is printed directly to stderr by the StreamHandler,
# then sent back to the ditscmd by the MsgOutHandler,
# which prints it again.
# Note also that long messages are automatically broken up
# into multiple shorter MsgOut commands by the MsgOutHandler.

[ryanb@koloa test]$ ditscmd PYEX_25322 GET_S 
2021-11-10 12:23:16,432 INFO PYEX_25322: GET_S(PYEX_25322, DATA, timeout=None)
DITSCMD_6301:PYEX_25322:INFO:GET_S(PYEX_25322, DATA, timeout=None)
2021-11-10 12:23:16,432 INFO PYEX_25322: GET_S reply: Message(DITSCMD_6301:DATA, GET_S, 0x11a6710, DITS_REA_COMPLETE, 0:ALL-S-OK, Status OK, ArgStructure, {'DATA': {'timestamp': 1636582996.2088468, 'number': 114, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]), 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}}, None)
DITSCMD_6301:PYEX_25322:INFO:GET_S reply: Message(DITSCMD_6301:DATA, GET_S, 0x11a6710, DITS_REA_COMPLETE, 0:ALL-S-OK, Status OK, ArgStructure, {'DATA': {'timestamp':
DITSCMD_6301:PYEX_25322: 1636582996.2088468, 'number': 114, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]), 'myarr': array([10., 20.,
DITSCMD_6301:PYEX_25322: 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}}, None)
2021-11-10 12:23:16,433 INFO PYEX_25322: GET_S DATA: {'timestamp': 1636582996.2088468, 'number': 114, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]), 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
DITSCMD_6301:PYEX_25322:INFO:GET_S DATA: {'timestamp': 1636582996.2088468, 'number': 114, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]),
DITSCMD_6301:PYEX_25322: 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
[ryanb@koloa test]$

# Calling GET_A with too short of a timeout.

[ryanb@koloa test]$ ditscmd PYEX_25322 GET_A timeout=0
2021-11-10 12:26:07,837 INFO PYEX_25322: GET_A(PYEX_25322, DATA, timeout=0.0)
DITSCMD_6319:PYEX_25322:INFO:GET_A(PYEX_25322, DATA, timeout=0.0)
2021-11-10 12:26:07,837 ERROR PYEX_25322: GET_A timeout after 0 seconds
##DITSCMD_6319:PYEX_25322:ERROR:GET_A timeout after 0 seconds
[ryanb@koloa test]$ 

# Calling MON.  The action keeps running until the ditscmd
# is killed with Ctrl-C, whereupon it gets the unexpected REA_DIED message,
# cancels the monitor, and stops.

[ryanb@koloa test]$ ditscmd PYEX_25322 MON
2021-11-10 12:26:53,283 INFO PYEX_25322: MON(PYEX_25322, DATA, timeout=None)
DITSCMD_6323:PYEX_25322:INFO:MON(PYEX_25322, DATA, timeout=None)
2021-11-10 12:26:53,283 INFO PYEX_25322: MON started, id=0
DITSCMD_6323:PYEX_25322:INFO:MON started, id=0
2021-11-10 12:26:53,283 INFO PYEX_25322: MON changed: {'timestamp': 1636583212.5887132, 'number': 330, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]), 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
DITSCMD_6323:PYEX_25322:INFO:MON changed: {'timestamp': 1636583212.5887132, 'number': 330, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]),
DITSCMD_6323:PYEX_25322: 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
2021-11-10 12:26:53,595 INFO PYEX_25322: MON changed: {'timestamp': 1636583213.5955217, 'number': 331, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]), 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
DITSCMD_6323:PYEX_25322:INFO:MON changed: {'timestamp': 1636583213.5955217, 'number': 331, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]),
DITSCMD_6323:PYEX_25322: 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
2021-11-10 12:26:54,596 INFO PYEX_25322: MON changed: {'timestamp': 1636583214.5957422, 'number': 332, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]), 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
DITSCMD_6323:PYEX_25322:INFO:MON changed: {'timestamp': 1636583214.5957422, 'number': 332, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]),
DITSCMD_6323:PYEX_25322: 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
2021-11-10 12:26:55,596 INFO PYEX_25322: MON changed: {'timestamp': 1636583215.5962248, 'number': 333, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]), 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
DITSCMD_6323:PYEX_25322:INFO:MON changed: {'timestamp': 1636583215.5962248, 'number': 333, 'mylist': array(['1', '2', 'a', 'b'], dtype='<U1'), 'mynums': array([1.2, 2.3, 3.4, 4.5]),
DITSCMD_6323:PYEX_25322: 'myarr': array([10., 20., 30.]), 'myfloat': 3.1415927410125732, 'mydict': {'x': 1, 'y': 2, 'z': 3}, 'mystring': 'hello'}
^C2021-11-10 12:26:56,175 ERROR PYEX_25322: MON unexpected msg: Message(???:DITSCMD_6323, MON, 0x0, DITS_REA_DIED, 0:ALL-S-OK, Status OK, None, None, None)
DITSCMD_6323:exit status:%DITS-F-SIGINT, DITS Exited via exit handler with signal SIGINT

# Using HELP to check the running actions -- MON is no longer active.

[ryanb@koloa test]$ 
[ryanb@koloa test]$ ditscmd PYEX_25322 HELP
DITSCMD_6333:PYEX_25322:Task PYEX_25322 has the following actions:
DITSCMD_6333:PYEX_25322:    MON
DITSCMD_6333:PYEX_25322:    GET_A
DITSCMD_6333:PYEX_25322:    GET_S
DITSCMD_6333:PYEX_25322:    PUB (Active)
DITSCMD_6333:PYEX_25322:    INITIALISE
DITSCMD_6333:PYEX_25322:    CONFIGURE
DITSCMD_6333:PYEX_25322:    SETUP_SEQUENCE
DITSCMD_6333:PYEX_25322:    SEQUENCE
DITSCMD_6333:PYEX_25322:    END_OBSERVATION
DITSCMD_6333:PYEX_25322:    END_OF_NIGHT
DITSCMD_6333:PYEX_25322:    DEBUG
DITSCMD_6333:PYEX_25322:    DEBUG_FILE
DITSCMD_6333:PYEX_25322:    TEST
DITSCMD_6333:PYEX_25322:    PING
DITSCMD_6333:PYEX_25322:    REPORT
DITSCMD_6333:PYEX_25322:    FREPORT
DITSCMD_6333:PYEX_25322:    TASK_REGISTER
DITSCMD_6333:PYEX_25322:    HELP (Active)
DITSCMD_6333:PYEX_25322:    EXIT
[ryanb@koloa test]$ 

# Killing the task gracefully using the EXIT command.

[ryanb@koloa test]$ ditscmd PYEX_25322 EXIT
2021-11-10 12:28:03,995 INFO drama: Exit(('DitsMsgReceive',), {}) sent PYEX_25322 EXIT
2021-11-10 12:28:03,995 INFO PYEX_25322: drama.stop()
[ryanb@koloa test]$ 
[1]    Done                          ./example.py
[ryanb@koloa test]$ 

'''
