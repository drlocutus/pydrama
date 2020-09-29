#!/local/python3/bin/python3
import sys
import os
import jac_sw
import drama

taskname = 'PYMON_' + str(os.getpid())

import drama.log
drama.log.setup()
import logging
log = logging.getLogger(taskname)
#logging.getLogger('drama').setLevel(logging.DEBUG)


def MON(msg):
    try:
        if msg.reason == drama.REA_OBEY:
            task = sys.argv[1]
            parm = sys.argv[2]
            log.info('MON %s %s', task, parm)
            drama.monitor(task, parm)
            drama.reschedule()
        elif msg.reason == drama.REA_COMPLETE:
            reply = msg.arg
            log.info('MON done: %s', reply)
        elif msg.reason == drama.REA_TRIGGER:
            if msg.status == drama.MON_STARTED:
                log.info('MON_STARTED')
                drama.reschedule()
            elif msg.status == drama.MON_CHANGED:
                log.info('MON_CHANGED: %s', msg.arg)
                drama.reschedule()
            else:
                log.error('MON unexpected TRIGGER status: %s', msg)
        else:
            log.error('GET_A unexpected msg: %s', msg)
    except:
        log.exception('MON exception')



try:
    log.info('drama.init(%s)', taskname)
    drama.init(taskname, actions=[MON])
    drama.blind_obey(taskname, 'MON')
    drama.run()
finally:
    log.info('drama.stop()')
    drama.stop()


