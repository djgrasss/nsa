__author__ = 'highland'

import sys,logging,os
from PyQt4 import QtGui,QtCore
import pyproj
import ui,controller
import _globals

def getViewerProj():
    return pyproj.Proj(proj='merc',ellps='WGS84')

def getCalcProj():
    return pyproj.Proj(proj='utm',zone = 50,ellps='WGS84')

class LogQListHandlerClass(logging.Handler):
    def __init__(self,level = logging.NOTSET):
        super(LogQListHandlerClass,self).__init__(level)
        self.messageQueue = _globals.getMessageQueue()

    def emit(self,record):
        if record.levelno >= self.level:
            self.messageQueue.put(record)

def main():

    FORMAT = '%(asctime)-15s %(levelname)s :%(message)s'
    formatter = logging.Formatter(fmt=FORMAT)
    log = logging.getLogger('global')
    log.setLevel(logging.DEBUG)
    FH_Path = os.path.join(os.path.split(__file__)[0],'nsa.log')
    FH = logging.FileHandler(FH_Path)
    FH.setFormatter(formatter)
    log.addHandler(FH)

    QueueLogHandler = LogQListHandlerClass()
    QueueLogHandler.setFormatter(formatter)
    QueueLogHandler.setLevel(logging.INFO)
    log.addHandler(QueueLogHandler)

    app = QtGui.QApplication(sys.argv)
    mainLogic = controller.MainController()
    mainWin = ui.MainWindow(controller=mainLogic)
    mainWin.controller.mainWindow = mainWin
    mainWin.show()

    sys.exit(app.exec_())

if __name__ == '__main__':
    main()
