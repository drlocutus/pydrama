#!/local/python3/bin/python3
'''
retry.py
RMB 20190826

Module supplying the RetryMonitor class,  which monitors a parameter in a
remote task and automatically attempts to reconnect as needed.
A RetryMonitor will typically be created as a static variable
in an action function (see example below).

About logging.  A custom-named log for each instance would simplify the
log messages a bit, but unfortunately MsgOutHandler format doesn't include
the logger name since it's assumed to be the same as the task.

ISSUE: If this subscriber task gets hung (e.g. ctrl-Z) long enough for the
       recv buffer to fill up, the publisher will start sending out
       notify messages.  When this subscriber task wakes up again,
       if it sends out a cancel message before first clearing out its
       input buffer, the PUBLISHER will segfault, apparently because it
       incorrectly looks for a transaction id on the cancel message.
       DITS_M_IMB_ROUND_ROBIN doesn't help; RESCHED still processes first.
       
       TODO: Verify this behavior with C tasks.
       
       WORKAROUND: Ignore the first RESCHED.  This effectively doubles the
                   user timeout on the monitor, but avoids killing the
                   publisher when we wake from sleep.
       
'''

if __name__ == "__main__":
    import jac_sw

import drama
import logging

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

class RetryMonitor(object):
    '''Monitor a remote parameter with retries on lost connections.
    
       Call the handle() method to deal with a new message;
       it will return True on a MON_CHANGED for this transaction.
       
       The RetryMonitor tracks connection status in the 'connected' bool.
       
       Behavior is customizable by reassigning the following variables
       to a different member function:
         * on_kick:    REA_KICK. default cancel().
         * on_resched: REA_RESCHED. default start().
         * on_orphan:  REA_DIED with transid=0 (parent task died).  default nop().
       
       Note that we get no message if the parent action completes and the
       parent task remains alive.
    '''
    
    def __init__(self, task, param):
        #self.log = logging.getLogger('RetryMonitor(%s.%s)'%(task,param))
        self.log = log
        self.task = task
        self.param = param
        self.tid = None
        self.mid = None
        self.connected = False
        self.resched_count = 1  # once started, delay restart until second resched
        # default handle() behavior for entry reasons.
        # override by assigning nop, cancel, or start.
        self.on_kick = self.cancel
        self.on_resched = self.start
        self.on_orphan = self.nop
    
    
    def nop(self, reason=''):
        '''Do nothing.  Used to ignore entry reasons in handle().'''
        pass
    
    
    def cancel(self, reason=''):
        '''Cancel the monitor if running.'''
        self.connected = False
        if self.mid is not None:
            self.log.debug('%s: drama.cancel(%s.%s, %d)', reason, self.task, self.param, self.mid)
            try:
                drama.cancel(self.task, self.mid)
            except drama.BadStatus as s:
                self.log.error('drama.cancel(%s.%s, %d) error: %s', self.task, self.param, self.mid, s)
            self.mid = None
        else:
            self.log.debug('%s: no MONITOR_ID to cancel on %s.%s', reason, self.task, self.param)
    
    
    def start(self, reason=''):
        '''Cancel monitor, then restart if no outstanding transaction.'''
        self.cancel(reason)
        if self.tid is None:
            self.log.debug('%s: drama.monitor(%s.%s)', reason, self.task, self.param)
            try:
                self.tid = drama.monitor(self.task, self.param)
            except drama.BadStatus as s:
                self.log.error('drama.monitor(%s.%s) error: %s', self.task, self.param, s)
        else:
            self.log.debug('%s: outstanding monitor transid on %s.%s', reason, self.task, self.param)
    
    
    def clear(self, reason=''):
        '''Set mid and tid to None.  Used for COMPLETE and DIED.'''
        self.log.debug('%s: clearing tid/mid for %s.%s', reason, self.task, self.param)
        self.tid = None
        self.mid = None
        self.connected = False
    
    
    def handle(self, msg):
        '''Handle drama.Message, retrying as needed.
           Return True on MON_CHANGED for this monitor.
           Behavior is controlled by the following instance variables:
                on_kick
                on_resched
        '''
        if msg.reason == drama.REA_OBEY:
            self.clear('OBEY')
            self.start('OBEY')
        elif msg.reason == drama.REA_KICK:
            self.on_kick('KICK')  # default cancel
        elif msg.reason == drama.REA_RESCHED:
            if self.resched_count:
                self.on_resched('RESCHED')  # default (re)start
            self.resched_count += 1
        elif msg.reason == drama.REA_DIED and msg.transid == 0:
            self.on_orphan('ORPHAN')  # default nop
        elif msg.reason == drama.REA_EXIT:
            self.cancel('EXIT')  # paranoia, we never see this reason
        elif msg.transid == self.tid:
            if msg.reason == drama.REA_COMPLETE:
                # normally only see this if remote task hangs for while,
                # we cancel, then it comes back.  so try immediate restart.
                self.clear('COMPLETE')
                self.start('COMPLETE')
            elif msg.reason == drama.REA_DIED:
                # depending on how the task died, we might see this message
                # on death OR rebirth.  but even on rebirth, an immediate
                # restart will get a CLOSECONN error.  wait for reschedule.
                self.clear('DIED')
            elif msg.reason == drama.REA_MESREJECTED:
                # drama might not be running on remote machine,
                # or the parameter might not exist in the remote task (yet).
                # output status (TODO warning?) and wait for resched.
                self.log.debug('MESREJECTED: monitor %s.%s status: %d: %s',
                               self.task, self.param, msg.status, drama.get_status_string(msg.status))
                self.clear('MESREJECTED')
            elif msg.reason == drama.REA_TRIGGER:
                self.resched_count = 0
                if msg.status == drama.MON_STARTED:
                    if self.mid is not None:
                        self.cancel('STARTED')  # paranoia
                    self.mid = msg.arg['MONITOR_ID']
                    self.connected = True
                    self.log.info('STARTED: %s.%s MONITOR_ID=%d', self.task, self.param, self.mid)
                elif msg.status == drama.MON_CHANGED:
                    self.connected = True
                    return True
                else:
                    self.log.warning('TRIGGER: monitor %s.%s unhandled status %d: %s',
                                    self.task, self.param, msg.status, drama.get_status_string(msg.status))
        
        return False
        # RetryMonitor.handle
        


if __name__ == "__main__":
    # RetryMonitor test
    import sys
    import os
    
    task = sys.argv[1]
    param = sys.argv[2]
    timeout = 5.0
    if len(sys.argv) > 3:
        timeout = float(sys.argv[3])
    
    taskname = 'RM_%d' % (os.getpid())
    
    import drama.log
    drama.log.setup()
    log.setLevel(logging.DEBUG)
    
    # change log name to taskname instead of __main__
    del log.manager.loggerDict[log.name]
    log.name = taskname
    log.manager.loggerDict[log.name] = log
    
    def MONITOR(msg):
        if not hasattr(MONITOR, 'retry_monitor'):
            MONITOR.retry_monitor = RetryMonitor(task,param)
            # ctrl-Z testing
            #MONITOR.retry_monitor.on_resched = MONITOR.retry_monitor.nop
        log.debug('%s', msg)
        if MONITOR.retry_monitor.handle(msg):
            log.info('%s.%s: %s', task, param, msg.arg)
        drama.reschedule(timeout)
    
    try:
        log.info('drama.init(%s)', taskname)
        drama.init(taskname, actions=[MONITOR]) #, flags=1024)  # DITS_M_IMB_ROUND_ROBIN
        drama.blind_obey(taskname, 'MONITOR')
        log.info('drama.run()...')
        drama.run()
    finally:
        log.info('drama.stop(%s)', taskname)
        drama.stop(taskname)
        log.info('done.')
    
    
