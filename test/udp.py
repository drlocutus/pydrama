#!/local/python3/bin/python3
'''
test/udp.py     RMB 20190708


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
import socket
import time

taskname = 'PYUDP_' + str(os.getpid())

import drama.log
drama.log.setup()
import logging
log = logging.getLogger(taskname)
logging.getLogger('drama').setLevel(logging.DEBUG)

def SLEEP(msg):
    '''
    This action enters an infinite wait().
    Wake it up with a kick.
    '''
    if msg.reason == drama.REA_OBEY:
        log.info('SLEEP obey, calling drama.wait()...')
        drama.wait()
    elif msg.reason == drama.REA_KICK:
        log.info('SLEEP kicked.')
    else:
        log.info('SLEEP other msg: %s', msg)


def udp_callback(fd):
    msg,addr = fd.recvfrom(1024)
    fd.sendto(b'echo %.3f: %s'%(time.time(),msg), addr)

try:
    log.info('drama.init(%s)', taskname)
    drama.init(taskname, actions=[SLEEP])
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('0.0.0.0', 0))
    log.info('udp port: %d', s.getsockname()[1])
    drama.register_callback(s, udp_callback)
    drama.run()
finally:
    log.info('drama.stop()')
    drama.stop()

