#!/local/python3/bin/python3
'''
test/mon.py     RMB 20190910


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

import sys
import os
import jac_sw
import drama
import time

taskname = 'PYMON_' + str(os.getpid())

import drama.log
drama.log.setup()
import logging
log = logging.getLogger(taskname)
#logging.getLogger('drama').setLevel(logging.DEBUG)


def MON(msg):
    try:
        if msg.reason == drama.REA_OBEY:
            if len(sys.argv) > 2:
                task = sys.argv[1]
                parm = sys.argv[2]
            else:
                task = taskname  # self
                parm = 'TIME'
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


def PUB(msg):
    drama.set_param('TIME', time.time())
    drama.reschedule(1)


try:
    log.info('drama.init(%s)', taskname)
    drama.init(taskname, actions=[MON, PUB])
    drama.blind_obey(taskname, 'PUB')
    drama.blind_obey(taskname, 'MON')
    drama.run()
finally:
    log.info('drama.stop()')
    drama.stop()


