#!/local/python3/bin/python3
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

