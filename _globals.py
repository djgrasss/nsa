__author__ = 'highland'

from Queue import Queue
import pyproj

messageQueue = Queue()
progressQueue = Queue()

def getMessageQueue():
    #global messageQueue
    return messageQueue

def getProgressQueue():
    #global progressQueue
    return progressQueue

def getViewerProj():
    return pyproj.Proj(proj='merc',ellps='WGS84')

def getCalcProj():
    return pyproj.Proj(proj='utm',zone = 50,ellps='WGS84')
