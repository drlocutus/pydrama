'''
DRAMA for PyQt4.

Author: Ryan Berthold, JAC

This module declares DramaWidget, a base class for PyQt4/DRAMA GUI tasks.

Example:

    import sys
    import drama
    from drama.qt4 import DramaWidget
    from PyQt4 import QtCore, QtGui

    class MyWidget(DramaWidget):
    
        def __init__(self, taskname):
            self.taskname = taskname
            drama.init(self.taskname, actions=[self.ACTION])
            drama.blind_obey(self.taskname, "ACTION", "hello!")
            super(MyWidget, self).__init__()
            self.show()
        
        def get_title(self, title):
            return str(title)
        
        def ACTION(self, msg):
            arg_list, arg_dict = drama.parse_argument(msg.arg)
            title = get_title(self, *arg_list, **arg_dict)
            drama.msgout("ACTION: %s" % (title))
            self.setWindowTitle(title)
            self.update()
    
    taskname = "QTASK"
    retval = 1
    try:
        app = QtGui.QApplication(sys.argv)
        mw = MyWidget(taskname)
        retval = app.exec_()
    finally:
        drama.stop(taskname)
    sys.exit(retval)
'''


import drama as _drama
from PyQt4 import QtCore as _QtCore
from PyQt4 import QtGui as _QtGui
import logging as _logging


# Logging config is left for the user
_log = _logging.getLogger(__name__)
_log.addHandler(_logging.NullHandler())  # avoid 'no handlers' exception


class DramaWidget(_QtGui.QWidget):
    '''
    DramaWidget is the base class for your DRAMA/PyQt4 GUI task.
    It creates QSocketNotifiers for each DRAMA file descriptor
    and calls drama.process_fd as each becomes available.
    
    Normally you should call drama.init() before creating a DramaWidget
    instance, but if necessary you can create the DramaWidget first
    and then invoke its check_fds() method after drama.init().
    '''
    
    def __init__(self):
        super(DramaWidget, self).__init__()
        self.task_fds = (set(), set(), set())
        self.task_notifiers = []
        self.check_fds()
    
    def on_fd(self, fd):
        '''
        Process this file descriptor,
        update the QSocketNotifier set if needed,
        and exit the QApplication if requested or on error.
        '''
        try:
            _drama.process_fd(fd)
            self.check_fds()
        except _drama.Exit:
            _QtCore.QCoreApplication.instance().quit()
        except:
            _log.exception('DramaWidget.on_fd(%s): unhandled exception' % (fd))
            _QtCore.QCoreApplication.instance().exit(1)
        
    def check_fds(self):
        '''Update the QSocketNotifier set if needed.'''
        # easiest just to reset everything on any change in fd's.
        fds = _drama.get_fd_sets()
        if fds != self.task_fds:
            _log.debug('DramaWidget.check_fds: updating fd sets.')
            self.task_fds = fds
            # disconnect all existing socket notifiers first
            for qsn in self.task_notifiers:
                qsn.activated.disconnect(self.on_fd)
                qsn.setEnabled(False)
                qsn.setParent(None)
                qsn.deleteLater()
            self.task_notifiers = []
            for fd in fds[0]:
                self.new_fd(fd, _QtCore.QSocketNotifier.Read)
            for fd in fds[1]:
                self.new_fd(fd, _QtCore.QSocketNotifier.Write)
            for fd in fds[2]:
                self.new_fd(fd, _QtCore.QSocketNotifier.Exception)

    def new_fd(self, fd, fdtype):
        '''Create/connect a new QSocketNotifier for given file descriptor.'''
        _log.debug('DramaWidget.new_fd(%d, %d)', fd, fdtype)
        qsn = _QtCore.QSocketNotifier(fd, fdtype, self)
        self.task_notifiers.append(qsn)
        qsn.activated.connect(self.on_fd)



