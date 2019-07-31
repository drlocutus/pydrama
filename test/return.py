#!/local/python3/bin/python3
'''
RMB 20190724
Tests returning a value from an action.

As it turns out, returning a value when the action has been rescheduled
has no effect (or at least a ditscmd won't see it).  The return value
only shows up when there has been no reschedule, or the reschedule
has been canceled using 'reschedule(False)' (which sets DITS_REQ_END).

This is relevant to rts.py FE clients, which need to return MULT.
Since this is normally done before rescheduling to wait for PTCS,
rts.py ought to cache return values for later.  The C version of
rtsDClient does this by making retArg a static variable.
'''
import jac_sw
import drama

taskname = "RETURN"

import drama.log
drama.log.setup()  # no file
import logging
log = logging.getLogger(taskname)
log.info('startup')


def CALLME(msg):
    if msg.reason == drama.REA_OBEY:
        drama.reschedule(3)
        #log.info('sleeping a bit...')
        drama.reschedule(False)
        return 42
    elif msg.reason == drama.REA_RESCHED:
        log.info('done.')
        return 88
    else:
        raise drama.BadStatus(drama.UNEXPMSG, 'unexpected msg: %s' % (msg))


try:
    log.info('drama.init(%s)', taskname)
    drama.init(taskname, actions=[CALLME])
    drama.run()
finally:
    log.info('drama.stop()')
    drama.stop()
