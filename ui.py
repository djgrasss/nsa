# -*- coding: cp936 -*
__author__ = 'highland'

import logging
import math
import urllib2
import os
import threading
import Queue
import csv
import re

#import NSA
import _globals

LAYER_SCAN = 200
LAYER_CELL = 300
LAYER_DYNAMIC = 100
LAYER_MAP = 0
from PyQt4 import QtCore,QtGui

FOCUSED_COLOR = QtGui.QColor(255,0,0,200)
COLOR900 = QtGui.QColor(255,215,0,200)
COLOR1800 = QtGui.QColor(135,206,250,200)
COLORUNKNOWN =QtGui.QColor(220,220,220,200)

STRONGEST_COLOR = QtGui.QColor(255,102,0,200)

STRONGEST_DASH_COLOR = QtGui.QColor(255,102,0,200)
NORMAL_DASH_COLOR = QtGui.QColor(255,204,0,200)
DASH_LINE_PEN = QtGui.QPen(QtCore.Qt.DashLine)
DASH_LINE_PEN.setColor(NORMAL_DASH_COLOR)
#DASH_LINE_PEN.setWidth(4)

STRONGEST_SOLID_COLOR = QtGui.QColor(0,153,0,200)
NORMAL_SOLID_COLOR = QtGui.QColor(0,153,255,200)
SOLID_LINE_PEN = QtGui.QPen(QtCore.Qt.SolidLine)
SOLID_LINE_PEN.setColor(NORMAL_SOLID_COLOR)
#SOLID_LINE_PEN.setWidth(8)

COARFCN_COLOR = QtGui.QColor(255,0,0,200)
COARFCN_PEN = QtGui.QPen(QtCore.Qt.SolidLine)
COARFCN_PEN.setColor(COARFCN_COLOR)

POLYGON_COLOR = QtGui.QColor(48,155,38,200)
POLYGON_PEN = QtGui.QPen(QtCore.Qt.SolidLine)
POLYGON_PEN.setColor(COARFCN_COLOR)


class NumberTableWidgetItem(QtGui.QTableWidgetItem):
    #For value based table column ordering

    def __init__(self,number):
        super(NumberTableWidgetItem,self).__init__('{}'.format(number))
        #self.setText()

    def __gt__(self, other):
        try:
            return float(self.text()) > float(other.text())
        except:
            try:
                return self.text() > other.text()
            except:
                return False

    def __lt__(self, other):
        try:
            return float(self.text()) < float(other.text())
        except:
            try:
                return self.text() < other.text()
            except:
                return False

    def value(self):
        try:
            if '.' in self.text():
                return float(self.text())
            else:
                return int(self.text())
        except:
            return None

class ScanedSSIInfoWidget(QtGui.QWidget):
    def __init__(self,parent = None):
        super(ScanedSSIInfoWidget,self).__init__(parent)
        label1 = QtGui.QLabel('Street covered Cells')
        label2 = QtGui.QLabel('Street covered TCHs')
        label3 = QtGui.QLabel('St. Structural Index')

        self.cellCountLabel = QtGui.QLabel('0')
        self.tchCountLabel = QtGui.QLabel('0')
        self.ssiCountLabel = QtGui.QLabel('0')

        self.cellCountLabel.setFrameShape(QtGui.QFrame.StyledPanel)
        self.tchCountLabel.setFrameShape(QtGui.QFrame.StyledPanel)
        self.ssiCountLabel.setFrameShape(QtGui.QFrame.StyledPanel)

        glayout = QtGui.QGridLayout()
        glayout.addWidget(label1,0,0)
        glayout.addWidget(self.cellCountLabel,0,1)
        glayout.addWidget(label2,1,0)
        glayout.addWidget(self.tchCountLabel,1,1)
        glayout.addWidget(label3,2,0)
        glayout.addWidget(self.ssiCountLabel,2,1)
        self.setFixedHeight(90)
        self.setLayout(glayout)

class ScanCoverageCriterionWidget(QtGui.QWidget):
    def __init__(self,parent = None):
        super(ScanCoverageCriterionWidget,self).__init__(parent)
        self.AbsoluteSpinBox = QtGui.QSpinBox()
        self.RelativeSpinBox = QtGui.QSpinBox()
        self.AvailableTchSpinBox = QtGui.QSpinBox()

        AbsolutePrefixLabel = QtGui.QLabel('RXLEV ACCESS MIN = -110 + ')
        self.AbsoluteSpinBox = QtGui.QSpinBox()
        self.AbsoluteSpinBox.setMinimum(0)
        self.AbsoluteSpinBox.setMaximum(63)
        self.AbsoluteSpinBox.setValue(15)
        AbsoluteSuffixLabel = QtGui.QLabel('dB')

        RelativePrefixLabel = QtGui.QLabel('Inferior to Strongest Rxlev')
        self.RelativeSpinBox = QtGui.QSpinBox()
        self.RelativeSpinBox.setMinimum(0)
        self.RelativeSpinBox.setMaximum(63)
        self.RelativeSpinBox.setValue(12)
        RelativeSuffixLabel = QtGui.QLabel('dB')

        AvailableTchPrefixLabel = QtGui.QLabel('Available TCH ARFCN Counts')
        self.AvailableTchSpinBox = QtGui.QSpinBox()
        self.AvailableTchSpinBox.setMinimum(1)
        self.AvailableTchSpinBox.setMaximum(250)
        self.AvailableTchSpinBox.setValue(70)
        #TemsScanReplotButton = QtGui.QPushButton('Plot')
        #TemsScanReplotButton.setFixedWidth(40)

        glayout = QtGui.QGridLayout()
        glayout.addWidget(AbsolutePrefixLabel,0,0)
        glayout.addWidget(self.AbsoluteSpinBox,0,1)
        glayout.addWidget(AbsoluteSuffixLabel,0,2)
        glayout.addWidget(RelativePrefixLabel,1,0)
        glayout.addWidget(self.RelativeSpinBox,1,1)
        glayout.addWidget(RelativeSuffixLabel,1,2)
        glayout.addWidget(AvailableTchPrefixLabel,2,0)
        glayout.addWidget(self.AvailableTchSpinBox,2,1)
        #glayout.addWidget(TemsScanReplotButton,2,2)
        self.setFixedHeight(90)
        self.setLayout(glayout)

class RFReplanOptionDialog(QtGui.QDialog):

    startRePlanRequestSignal = QtCore.pyqtSignal(str,list,list)
    selectOnGISRequestSignal = QtCore.pyqtSignal(object)

    def __init__(self,parent = None):
        super(RFReplanOptionDialog,self).__init__(parent)
        self.setWindowTitle('RF RePlaning Macro Site')
        self.logger = logging.getLogger('global')
        self.re = re.compile('[0-9][0-9][0-9]\-[0-9][0-9]\-[0-9]+\-[0-9]+')


        self.ArfcnChooseTable = QtGui.QTableWidget()
        self.ArfcnChooseTable.setColumnCount(8)
        self.ArfcnChooseTable.setRowCount(128)

        for y in range(128):
            for x in range(8):
                item = NumberTableWidgetItem(y*8+x+1)
                self.ArfcnChooseTable.setItem(y,x,item)

        self.ArfcnChooseTable.setFixedWidth(330)
        self.ArfcnChooseTable.setFixedHeight(350)
        self.ArfcnChooseTable.resizeColumnsToContents()
        self.ArfcnChooseTable.resizeRowsToContents()
        self.ArfcnChooseTable.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)

        self.ArfcnListForBCCH_Macro = QtGui.QListWidget()
        self.ArfcnListForBCCH_Macro.setFixedWidth(80)
        self.ArfcnListForBCCH_Macro.setDisabled(True)
        self.ArfcnListForBCCH_Macro.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.ArfcnListForBCCH_Macro.setSelectionMode(QtGui.QAbstractItemView.ExtendedSelection)



        self.ArfcnListForTCH_Macro = QtGui.QListWidget()
        self.ArfcnListForTCH_Macro.setFixedWidth(80)
        self.ArfcnListForTCH_Macro.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.ArfcnListForTCH_Macro.setSelectionMode(QtGui.QAbstractItemView.ExtendedSelection)

        self.CellList = QtGui.QListWidget()
        self.CellList.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        self.CellList.setSelectionMode(QtGui.QAbstractItemView.ExtendedSelection)
        self.CellList.setFixedWidth(150)

        BB = QtGui.QDialogButtonBox()
        btStart = BB.addButton('Start RePlan',QtGui.QDialogButtonBox.NoRole)
        self.connect(btStart,QtCore.SIGNAL('clicked(bool)'),self.startRePlan)

        glayout = QtGui.QGridLayout()
        glayout.addWidget(QtGui.QLabel('Avail ARFCN'),0,0)
        glayout.addWidget(QtGui.QLabel('BCCH ARFCN'),0,1)
        glayout.addWidget(QtGui.QLabel('TCH ARFCN'),0,2)
        glayout.addWidget(QtGui.QLabel('Target Cells'),0,3)
        glayout.addWidget(self.ArfcnChooseTable,1,0)
        glayout.addWidget(self.ArfcnListForBCCH_Macro,1,1)
        glayout.addWidget(self.ArfcnListForTCH_Macro,1,2)
        glayout.addWidget(self.CellList,1,3)

        hlayout = QtGui.QHBoxLayout()
        self.reportPathEdit = QtGui.QLineEdit()
        reportPathLabel = QtGui.QLabel('RePlan result output path')
        chooseBT = QtGui.QPushButton('Choose')
        self.connect(chooseBT,QtCore.SIGNAL('clicked(bool)'),self.setRePlanReportPath)
        hlayout.addWidget(reportPathLabel)
        hlayout.addWidget(self.reportPathEdit)
        hlayout.addWidget(chooseBT)

        vlayout = QtGui.QVBoxLayout()
        vlayout.addLayout(glayout)
        vlayout.addLayout(hlayout)
        vlayout.addWidget(BB)
        self.setLayout(vlayout)

        clearFromTCHListAction = MainWindow.createAction(self.parent(),'Clear selected ARFCNs',self.removeFromTCHList)
        clearFromCellListAction = MainWindow.createAction(self.parent(),'Clear selected Cells',self.removeFromCellList)
        loadAction = MainWindow.createAction(self.parent(),'Load From file...',self.loadFromCSV)
        exportAction = MainWindow.createAction(self.parent(),'Export to file...',self.exportToCSV)
        selectAction = MainWindow.createAction(self.parent(),'Select from GIS Object',self.onSelectOnGISRequest)
        addToTCHAction = MainWindow.createAction(self.parent(),'Add To avail Macro TCH list',self.addArfcnToList)

        self.parent().addActions(self.CellList,(loadAction,exportAction,selectAction,clearFromCellListAction))
        self.parent().addActions(self.ArfcnListForTCH_Macro,(clearFromTCHListAction,))
        self.parent().addActions(self.ArfcnChooseTable,(addToTCHAction,))

        #self.connect(btAddToTch,QtCore.SIGNAL('clicked(bool)'),self.addArfcnToList)

    def setRePlanReportPath(self):
        filePath = QtGui.QFileDialog.getSaveFileName(filter = 'CSV File (*.csv)')
        if filePath:
            self.reportPathEdit.setText(filePath)

    def addArfcnToList(self):
        self.logger.debug('clicked.')
        self.ArfcnListForTCH_Macro.addItems(['{}'.format(item.value()) for item in self.ArfcnChooseTable.selectedItems()])
        self.ArfcnListForBCCH_Macro.sortItems()

    def removeFromTCHList(self):
        self.logger.debug('clicked')
        rowidx = [idx.row() for idx in self.ArfcnListForTCH_Macro.selectedIndexes()]
        rowidx.sort(reverse = True)
        for i in rowidx:
            self.logger.debug('item row {}'.format(i))
            self.ArfcnListForTCH_Macro.takeItem(i)

    def removeFromCellList(self):
        self.logger.debug('clicked')
        rowidx = [idx.row() for idx in self.CellList.selectedIndexes()]
        rowidx.sort(reverse = True)
        for i in rowidx:
            self.logger.debug('item row {}'.format(i))
            self.CellList.takeItem(i)

    def loadFromCSV(self):
        fileName = QtGui.QFileDialog.getOpenFileName(filter = 'CSV File (*.csv)')
        if unicode(fileName):
            with open(unicode(fileName),'rb') as fp:
                reader = csv.reader(fp)
                cgi = set(['{}'.format(row[0]) for row in reader if self.re.match(row[0])])
                self.CellList.addItems(list(cgi))

    def exportToCSV(self):
        fileName = QtGui.QFileDialog.getSaveFileName(filter = 'CSV File (*.csv)')
        if unicode(fileName):
            with open(unicode(fileName),'wb') as fp:
                writer = csv.writer(fp)
                data = [[self.CellList.item(i).text(),] for i in range(self.CellList.count())]
                writer.writerow(['CGI'])
                writer.writerows(data)

    def onSelectOnGISRequest(self):
        self.selectOnGISRequestSignal.emit(self.onSelectOnGISResponse)
        self.close()

    def onSelectOnGISResponse(self,cgis):
        self.logger.debug('cgi got {}'.format(cgis))
        self.CellList.addItems(cgis)
        self.show()

    def startRePlan(self):
        #TODO
        availArfcns = []
        targetCells = []
        try:
            availArfcns = [int(self.ArfcnListForTCH_Macro.item(i).text()) for i in range(self.ArfcnListForTCH_Macro.count())]
        except:
            self.logger.error('Unexpected ARFCN in list!')

        try:
            targetCells = [unicode(self.CellList.item(i).text()) for i in range(self.CellList.count())]
        except:
            self.logger.error('Unexpected ARFCN in list!')

        if self.reportPathEdit.text() and availArfcns and targetCells:
            self.startRePlanRequestSignal.emit(unicode(self.reportPathEdit.text()),availArfcns,targetCells)
            self.close()
        else:
            QtGui.QMessageBox.critical(self,'Error','Please full fill the required field!')
            self.logger.error('Please full fill the required field!')

class MaximalConnectedClusterDialog(QtGui.QDialog):
    def __init__(self,parent = None):
        super(MaximalConnectedClusterDialog,self).__init__(parent)
        MatchedMRLabel = QtGui.QLabel('Path for matched MR records')
        self.MatchedMREdit = QtGui.QLineEdit()
        MatchedMRLabel.setBuddy(self.MatchedMREdit)
        MatchedMRButton = QtGui.QPushButton('Choose')

        ClusterLabel = QtGui.QLabel('Path for cluster report')
        self.ClusterEdit = QtGui.QLineEdit()
        ClusterLabel.setBuddy(self.ClusterEdit)
        ClusterButton = QtGui.QPushButton('Choose')

        bb = QtGui.QDialogButtonBox()
        #self.progress = QtGui.QProgressBar()
        self.runButton = bb.addButton('Run',QtGui.QDialogButtonBox.ApplyRole)
        #self.closeButton = bb.addButton('Close',QtGui.QDialogButtonBox.RejectRole)

        glayout = QtGui.QGridLayout()
        glayout.addWidget(MatchedMRLabel,0,0)
        glayout.addWidget(self.MatchedMREdit,0,1)
        glayout.addWidget(MatchedMRButton,0,2)
        glayout.addWidget(ClusterLabel,1,0)
        glayout.addWidget(self.ClusterEdit,1,1)
        glayout.addWidget(ClusterButton,1,2)

        vlayout = QtGui.QVBoxLayout()
        vlayout.addLayout(glayout)

        vlayout.addWidget(bb)
        #vlayout.addWidget(self.progress)

        self.connect(MatchedMRButton,QtCore.SIGNAL('clicked(bool)'),self.chooseMatchMrPath)
        self.connect(ClusterButton,QtCore.SIGNAL('clicked(bool)'),self.chooseClusterPath)
        self.connect(self.runButton,QtCore.SIGNAL('clicked(bool)'),self.startCalc)
        #self.connect(self.closeButton,QtCore.SIGNAL('clicked(bool)'),self.close)

        self.setLayout(vlayout)
        self.setWindowTitle('Maximal Connected Cluster Check Report')
        self.workingThread = None

    def chooseMatchMrPath(self):
        fileName = QtGui.QFileDialog.getSaveFileName(self,filter = 'CSV Files (*.csv)')
        if fileName:
            self.MatchedMREdit.setText(fileName)

    def chooseClusterPath(self):
        fileName = QtGui.QFileDialog.getSaveFileName(self,filter = 'CSV Files (*.csv)')
        if fileName:
            self.ClusterEdit.setText(fileName)

    def startCalc(self):
        self.workingThread = threading.Thread(target = self.parent().controller.calcSaveMaximalConnectedClusterReport,name = 'clusterCalc',args=( ( unicode(self.MatchedMREdit.text()),unicode(self.ClusterEdit.text()) ) ) )
        self.runButton.setDisabled(True)
        self.workingThread.start()
        self.close()

class GISGraphicView(QtGui.QGraphicsView):

    itemDetectedSignal = QtCore.pyqtSignal(QtGui.QGraphicsItem,QtCore.QPointF)
    CursorLocationSignal  = QtCore.pyqtSignal(tuple)
    CellRMenuRequestSignal = QtCore.pyqtSignal(QtGui.QMouseEvent,QtGui.QGraphicsItem)
    polygonSelectCompleteSignal = QtCore.pyqtSignal(list)

    mercProj = _globals.getViewerProj()
    utmProj = _globals.getCalcProj()

    TDIM = 256

    def __init__(self,parent = None):
        super(GISGraphicView,self).__init__(parent)
        self.logger = logging.getLogger('global')
        #self.CalcZoomScaleMapping()
        self.scale(0.01,0.01)
        self.cursorMode = None
        self.verticalTraceLine = None
        self.horizontalTraceLine = None
        self.preDetectedItem = None
        self.preDetectedItemBrush = None
        self.preDetectedItemOrgBrush = None
        self.setDragMode(QtGui.QGraphicsView.ScrollHandDrag)
        #self.zoomLevel = self.getRequiredZoomLevel()[0]
        self.zoomLevel = None
        self.startTile = None
        self.maxTiles = None
        self.mapLoader = OSMBackgroundLabor(self,parent = self)
        self.mapLoader.start()
        self.mapTiles = {}
        self.pixCaches = {}
        self.prePolygonLines = []
        self.prePolygonPoints = []
        self.PolygonStartPoint = None
        self.preQMousePos = None
        self.currentPolygonLines = None

        self.polygonSelectMenu = QtGui.QMenu(self)
        self.markAction = MainWindow.createAction(self.parent(),'Mark',self.onMark)
        self.completePolygonSelectionAction = MainWindow.createAction(self.parent(),'Complete Selection',self.onCompletePolygonSelection)
        self.cancelPolygonSelectModeAction = MainWindow.createAction(self.parent(),'Cancel Selection',self.onCancelPolygonSelection)
        MainWindow.addActions(self.parent(),self.polygonSelectMenu,(self.markAction,self.completePolygonSelectionAction,self.cancelPolygonSelectModeAction))

    def disableBackgroundMap(self):
        self.mapLoader.fetchCompleteSignal.disconnect(self.drawTile)
        for zl in self.mapTiles:
            for toDestory in self.mapTiles[zl].itervalues():
                self.scene().removeItem(toDestory)
                self.mapTiles[zl] = {}
        self.logger.info('Background map disabled')

    def enableBackgroundMap(self):
        self.mapLoader.fetchCompleteSignal.connect(self.drawTile)
        self.logger.info('Background map enabled')

    def enablePolygonSelectionMode(self):
        if self.prePolygonLines:
            for item in self.prePolygonLines:
                self.scene().removeItem(item)
        self.cursorMode = 'PolygonSelect'
        self.setDragMode(QtGui.QGraphicsView.NoDrag)
        self.setCursor(QtGui.QCursor(QtCore.Qt.PointingHandCursor))
        self.logger.info('Enter polygon selection mode.')

    def onMark(self):
        if self.preQMousePos:
            self.logger.debug('viewer point {},scene point {}'.format(self.preQMousePos,self.mapToScene(self.preQMousePos)))
            self.prePolygonPoints.append(self.mapToScene(self.preQMousePos))
            if len(self.prePolygonPoints) > 1:
                self.prePolygonLines.append(self.scene().addLine(self.prePolygonPoints[-2].x(),self.prePolygonPoints[-2].y(),self.prePolygonPoints[-1].x(),self.prePolygonPoints[-1].y(),POLYGON_COLOR))
            else:
                self.PolygonStartPoint = self.scene().addEllipse(self.prePolygonPoints[0].x()-4,self.prePolygonPoints[0].y()-4,8,8,POLYGON_COLOR)
            self.preQMousePos = None
        else:
            self.logger.error("Don't know where to mark")

    def onCompletePolygonSelection(self):
        cgis = []
        itemsToToggle = []
        if len(self.prePolygonPoints) > 2:
            selectPloygon = QtGui.QPolygonF(self.prePolygonPoints)
            if selectPloygon:
                items  = self.scene().items(selectPloygon)
                for item in items:
                    if hasattr(item,'logicalType') and getattr(item,'logicalType') == 'cgi':
                        cgis.append(item.logicalId)
                        itemsToToggle.append(item)
                self.scene().toggleGraphicItemHighlightStatus(itemsToToggle)
        self.cursorMode = None
        for item in self.prePolygonLines:
            self.scene().removeItem(item)
        self.scene().removeItem(self.PolygonStartPoint)
        self.prePolygonPoints = []
        self.prePolygonLines = []
        self.PolygonStartPoint = None
        self.preQMousePos = None
        self.polygonSelectCompleteSignal.emit(cgis)
        self.setDragMode(QtGui.QGraphicsView.ScrollHandDrag)
        self.logger.info('Exit polygon selection mode with result.')


    def onCancelPolygonSelection(self):
        self.cursorMode = None
        for item in self.prePolygonLines:
            self.scene().removeItem(item)
        self.scene().removeItem(self.PolygonStartPoint)
        self.prePolygonPoints = []
        self.prePolygonLines = []
        self.PolygonStartPoint = None
        self.preQMousePos = None
        self.polygonSelectCompleteSignal.emit([])
        self.setDragMode(QtGui.QGraphicsView.ScrollHandDrag)
        self.logger.info('Exit polygon selection mode on cancel.')


    def wheelEvent(self, QWheelEvent):
        factor = 1.41 ** (-QWheelEvent.delta()/240.0)
        self.scale(factor,factor)
        self.zoomLevel,self.startTile,self.maxTiles = self.getRequiredZoomLevel()
        self.mapLoader.missionIncome.wakeAll()

    def mouseMoveEvent(self, QMouseEvent):
        #self.logger.debug('mouseMoveEvent detected')
        super(GISGraphicView,self).mouseMoveEvent(QMouseEvent)

        eventPos = self.mapToScene(QMouseEvent.x(),QMouseEvent.y())
        self.CursorLocationSignal.emit(self.mercProj(eventPos.x(),-1*eventPos.y(),inverse = True))

        if self.cursorMode == 'traceObject':
            #self.logger.debug('mouseMoveEvent detected in traceObject mode,at {} {}'.format(QMouseEvent.pos().x(),QMouseEvent.pos().y()))
            sceneTopPointF = self.mapToScene(QMouseEvent.x(),0)
            sceneBottomPointF = self.mapToScene(QMouseEvent.x(),self.height())
            sceneLeftPointF = self.mapToScene(0,QMouseEvent.y())
            sceneRightPointF = self.mapToScene(self.width(),QMouseEvent.y())
            try:
                if self.verticalTraceLine:
                    self.scene().removeItem(self.verticalTraceLine)
            except:
                pass
            finally:
                self.verticalTraceLine = self.scene().addLine(sceneTopPointF.x(),sceneTopPointF.y(),sceneBottomPointF.x(),sceneBottomPointF.y())
                self.verticalTraceLine.setZValue(LAYER_DYNAMIC)
            try:
                if self.horizontalTraceLine:
                    self.scene().removeItem(self.horizontalTraceLine)
            except:
                pass
            finally:
                self.horizontalTraceLine = self.scene().addLine(sceneLeftPointF.x(),sceneLeftPointF.y(),sceneRightPointF.x(),sceneRightPointF.y())
                self.horizontalTraceLine.setZValue(LAYER_DYNAMIC)

            detectedItem = self.items(QMouseEvent.pos())
            while detectedItem:
                item = detectedItem.pop()
                if hasattr(item,'logicalType') and item != self.preDetectedItem:
                    #TODO NEED FIX WHEN focused item is highlighted item
                    if self.preDetectedItem:
                        #If any change back to orginal color
                        self.preDetectedItem.setBrush(self.preDetectedItemBrush)

                    #Replace focused item with focusColor
                    self.preDetectedItem = item
                    self.preDetectedItemBrush = item.brush()

                    focusedBrush = QtGui.QBrush(self.preDetectedItemBrush)
                    focusedBrush.setColor(FOCUSED_COLOR)
                    focusedBrush.setStyle(QtCore.Qt.SolidPattern)
                    item.setBrush(focusedBrush)

                    self.itemDetectedSignal.emit(item,self.mapToScene(QMouseEvent.x(),QMouseEvent.y()))
                    break
        else:
            self.preDetectedItem = None

    def mousePressEvent(self, QMouseEvent):
        if self.cursorMode == 'traceObject':
            if QMouseEvent.button() == QtCore.Qt.RightButton:
                detectedItem = self.items(QMouseEvent.pos())
                while detectedItem:
                    item = detectedItem.pop()
                    if hasattr(item,'logicalType') and getattr(item,'logicalType') == 'cgi':
                        self.CellRMenuRequestSignal.emit(QMouseEvent,item)
                        break
            else:
                self.setDragMode(QtGui.QGraphicsView.ScrollHandDrag)
        elif self.cursorMode == 'PolygonSelect':
            if QMouseEvent.button() == QtCore.Qt.RightButton:
                self.logger.debug('Right clicked in PolygonSelect mode.')
                self.polygonSelectMenu.popup(QMouseEvent.globalPos())
                self.preQMousePos = QMouseEvent.pos()
                self.logger.debug('right click at {},global {}'.format(QMouseEvent.pos(),QMouseEvent.globalPos()))
            else:
                self.setDragMode(QtGui.QGraphicsView.ScrollHandDrag)
        super(GISGraphicView,self).mousePressEvent(QMouseEvent)

    def mouseReleaseEvent(self, QMouseEvent):
        if self.cursorMode in ('traceObject','PolygonSelect'):
            self.setDragMode(QtGui.QGraphicsView.NoDrag)
        super(GISGraphicView,self).mouseReleaseEvent(QMouseEvent)
        if self.zoomLevel:
            if (self.zoomLevel,self.startTile,self.maxTiles) != self.getRequiredZoomLevel():
                self.mapLoader.missionIncome.wakeAll()

    def deg2num(self,lon_deg,lat_deg, zoom):
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        xtile = int((lon_deg + 180.0) / 360.0 * n)
        ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
        return (xtile, ytile)

    def num2deg(self,xtile, ytile, zoom):
        n = 2.0 ** zoom
        lon_deg = xtile / n * 360.0 - 180.0
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
        lat_deg = math.degrees(lat_rad)
        return (lon_deg,lat_deg)

    def getRequiredZoomLevel(self):
        maxCol = math.ceil(self.width() / self.TDIM)
        maxRow = math.ceil(self.height() / self.TDIM)
        maxTiles = int(max(maxCol,maxRow))
        leftUpCoords = self.mercProj(self.mapToScene(0,0).x(),-1*self.mapToScene(0,0).y() ,inverse = True)
        rightBottomCoords = self.mercProj(self.mapToScene(self.width(),self.height()).x(),-1*self.mapToScene(self.width(),self.height()).y() ,inverse = True)
        zoomLevel = 1
        for zoom in range(31,1,-1):
            startTile = self.deg2num(leftUpCoords[0],leftUpCoords[1],zoom)
            startTileCoords = self.num2deg(startTile[0],startTile[1],zoom)
            endTileCoords = self.num2deg(startTile[0]+ maxTiles+1,startTile[1] + maxTiles +1,zoom)
            if startTileCoords[0] <= leftUpCoords[0] and startTileCoords[1] >= leftUpCoords[1] and endTileCoords[0] >= rightBottomCoords[0] and endTileCoords[1] <= rightBottomCoords[1]:
                zoomLevel = zoom
                break
        else:
            self.logger.info('I don not know how to explain. minimum zoom not satisfy.')

        self.zoomLevel = zoomLevel
        self.startTile = startTile
        self.maxTiles = maxTiles
        #self.logger.debug('Current zoom level required {}'.format(zoomLevel))
        return zoomLevel,startTile,maxTiles

    def drawTile(self,path,pos,scale,zoomLevel):
        #self.logger.debug('Paint {} at {},scale {}'.format(path,pos,scale))
        pixKey = '{}.{}'.format(zoomLevel,pos)
        if pixKey in self.pixCaches:
            pass
        else:
            self.pixCaches[pixKey] = QtGui.QPixmap(path)

        if self.mapTiles.get(zoomLevel):
            if pixKey not in self.mapTiles[zoomLevel]:
                tileItem = self.scene().addPixmap(self.pixCaches[pixKey])
                tileItem.setPos(*pos)
                tileItem.setScale(scale)
                tileItem.setZValue(LAYER_MAP)
                self.mapTiles[zoomLevel][pixKey] = tileItem
            else:
                pass
        else:
            tileItem = self.scene().addPixmap(self.pixCaches[pixKey])
            tileItem.setPos(*pos)
            tileItem.setScale(scale)
            tileItem.setZValue(LAYER_MAP)
            self.mapTiles[zoomLevel] = {}
            self.mapTiles[zoomLevel][pixKey] = tileItem

        for zl in self.mapTiles:
            if zl != self.zoomLevel:
                for toDestory in self.mapTiles[zl].itervalues():
                    self.scene().removeItem(toDestory)
                self.mapTiles[zl] = {}
        #self.logger.debug('{} painted.'.format(path))

class GISScene(QtGui.QGraphicsScene):
    def __init__(self,parent = None):
        super(GISScene,self).__init__(parent)
        self.GraphicsItemLogicalMapping = {}
        self.GraphicsItemCellTraceLines = []
        self.preHighlightedItems = []
        self.preHighlightedBrushes = []
        self.itemOriginalBrush = Queue.Queue()

    def clearCellTraceLine(self):
        if self.GraphicsItemCellTraceLines:
            for item in self.GraphicsItemCellTraceLines:
                self.removeItem(item)
            self.GraphicsItemCellTraceLines = []

    def toggleGraphicItemHighlightStatus(self,toBeToggledItems = []):
        highlightBrush = QtGui.QBrush(QtGui.QColor(182,27,193,200))
#        while True:
#            try:
#                pair  = self.itemOriginalBrush.get_nowait()
#            except Queue.Empty:
#                break
#            try:
#                pair[0].setBrush(pair[1])
#            except:
#                self.logger.exception('Unexpected error occured when restore pre-highlighted item to its orginal brush.item {}'.format(pair[0]))
#
#        for item in toBeToggledItems:
#            self.itemOriginalBrush.put((item,item.brush()))
#            item.setBrush(highlightBrush)

        for item in self.preHighlightedItems:
            try:
                item.setBrush(self.preHighlightedBrushes[self.preHighlightedItems.index(item)])
            except:
                self.logger.exception('Unexpected error occured when restore pre-highlighted item to its orginal brush.')

        preHighlightedBrushes = []
        preHighlightedItems = []
        for item in toBeToggledItems:
            if item in self.preHighlightedItems:
                #In case that item already in highlight mode,preserve the brush before last highlight status
                preHighlightedBrushes.append(self.preHighlightedBrushes[self.preHighlightedItems.index(item)])
            else:
                preHighlightedBrushes.append(item.brush())
            preHighlightedItems.append(item)
            item.setBrush(highlightBrush)

        self.preHighlightedBrushes = preHighlightedBrushes
        self.preHighlightedItems = preHighlightedItems


class MessageQueueThread(QtCore.QThread):
    messageIncomeEvent = QtCore.pyqtSignal(str)

    def __init__(self,parent = None):
        super(MessageQueueThread,self).__init__(parent)
        self.messageQueue = _globals.getMessageQueue()
        self.runnable = True

    def run(self):
        #print 'message queue started.'
        while self.runnable:
            #print 'waiting for new log'
            record = self.messageQueue.get(True)
            #print 'get one'
            if type(record) is logging.LogRecord:
                msg = u'{} {}'.format(record.asctime,record.getMessage())
            else:
                msg = record
            self.messageIncomeEvent.emit(msg)

class ProgressQueueThread(QtCore.QThread):
    progressEvent = QtCore.pyqtSignal(int)

    def __init__(self,parent = None):
        super(ProgressQueueThread,self).__init__(parent)
        self.Queue = _globals.getProgressQueue()
        self.runnable = True

    def run(self):
        while self.runnable:
            #print 'wait'
            value = self.Queue.get(True)
            #print value
            if value != None:
                self.progressEvent.emit(int(value))

class LogInfoListWidget(QtGui.QListWidget):

    def __init__(self,parent = None,maxRecords = 1000):
        super(LogInfoListWidget,self).__init__(parent)
        self.maxRecords = maxRecords

    def log(self,record):
        if self.count() > self.maxRecords:
            self.takeItem(self.count())
        self.insertItem(0,record)
        if not self.isVisible():
            self.parent().show()
        self.parent().raise_()

class NewProjectDialog(QtGui.QDialog):
    def __init__(self,parent):
        super(NewProjectDialog,self).__init__(parent)

        ProjectNameLabel = QtGui.QLabel('Project Name')
        self.ProjectNameEdit = QtGui.QLineEdit()
        ProjectNameLabel.setBuddy(self.ProjectNameEdit)

        ProjectPathLabel = QtGui.QLabel('Project Path')
        self.ProjectPathEdit = QtGui.QLineEdit()
        ProjectPathLabel.setBuddy(self.ProjectPathEdit)
        ProjectPathButton = QtGui.QPushButton('Choose')

        ProjectUTMZoneLable = QtGui.QLabel('Project UTM Zone')
        self.ProjectUTMZoneSpinBox = QtGui.QSpinBox()
        self.ProjectUTMZoneSpinBox.setValue(50)
        self.ProjectUTMZoneSpinBox.setMinimum(0)
        ProjectUTMZoneLable.setBuddy(self.ProjectUTMZoneSpinBox)

        buttonBox = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Save|QtGui.QDialogButtonBox.Cancel)

        gridLayout = QtGui.QGridLayout()

        row = gridLayout.rowCount()
        gridLayout.addWidget(ProjectNameLabel,row,0)
        gridLayout.addWidget(self.ProjectNameEdit,row,1)

        row = gridLayout.rowCount()
        gridLayout.addWidget(ProjectPathLabel,row,0)
        gridLayout.addWidget(self.ProjectPathEdit,row,1)
        gridLayout.addWidget(ProjectPathButton,row,2)

        row = gridLayout.rowCount()
        gridLayout.addWidget(ProjectUTMZoneLable,row,0)
        gridLayout.addWidget(self.ProjectUTMZoneSpinBox,row,1)

        #row = gridLayout.rowCount()
        #gridLayout.addWidget(buttonBox,row,0)

        VLayout = QtGui.QVBoxLayout()
        VLayout.addLayout(gridLayout)
        #VLayout.addChildLayout(gridLayout)
        VLayout.addWidget(buttonBox)

        self.setLayout(VLayout)
        self.setWindowTitle('Create new project')

        self.connect(buttonBox,QtCore.SIGNAL('accepted()'),self,QtCore.SLOT('accept()'))
        self.connect(buttonBox,QtCore.SIGNAL('rejected()'),self,QtCore.SLOT('reject()'))
        self.connect(ProjectPathButton,QtCore.SIGNAL('clicked()'),self.selectSavePath)

    def acceptRoute(self):
        self.close()

    def selectSavePath(self):
        path = QtGui.QFileDialog.getSaveFileName(filter = 'NSA database (*.db3)')
        if path:
            self.ProjectPathEdit.setText(path)

#class OpenProjectDialog(QtGui.QDialog):
#    def __init__(self,projectPath ,parent=None):
#        super(OpenProjectDialog,self).__init__(parent)
#        self.setWindowTitle('Open project')
#        #dbConn = sqlite3.

class CellCoveragePerformanceReportDialog(QtGui.QDialog):
    def __init__(self,parent = None):
        super(CellCoveragePerformanceReportDialog,self).__init__(parent)

        glayout = QtGui.QGridLayout()
        glayout.addWidget(QtGui.QLabel('Interfering RXLEV threshold  inferior to strongest signal'),0,0)
        glayout.addWidget(QtGui.QLabel('Dominated RXLEV threshold inferior to strongest signal'),1,0)
        glayout.addWidget(QtGui.QLabel('Absolute ACC-MIN_RXLEV = -110 + '),2,0)

        glayout.addWidget(QtGui.QLabel('Interfering RXLEV threshold  inferior to strongest signal(SSI)'),3,0)
        glayout.addWidget(QtGui.QLabel('Available TCH frequency number count'),4,0)
        glayout.addWidget(QtGui.QLabel('Critical SSI threshold'),5,0)


        self.iRxlevCellSpinBox =  QtGui.QSpinBox()
        self.iRxlevCellSpinBox.setMaximum(63)
        self.iRxlevCellSpinBox.setMinimum(0)
        self.iRxlevCellSpinBox.setValue(12)
        self.dRxlevCellSpinBox =  QtGui.QSpinBox()
        self.dRxlevCellSpinBox.setMaximum(63)
        self.dRxlevCellSpinBox.setMinimum(0)
        self.dRxlevCellSpinBox.setValue(4)
        self.aRxlevCellSpinBox =  QtGui.QSpinBox()
        self.aRxlevCellSpinBox.setMaximum(63)
        self.aRxlevCellSpinBox.setMinimum(0)
        self.aRxlevCellSpinBox.setValue(15)
        self.iRxlevSSISpinBox = QtGui.QSpinBox()
        self.iRxlevSSISpinBox.setMaximum(63)
        self.iRxlevSSISpinBox.setMinimum(0)
        self.iRxlevSSISpinBox.setValue(12)
        self.availTchCountSpinBox = QtGui.QSpinBox()
        self.availTchCountSpinBox.setMaximum(250)
        self.availTchCountSpinBox.setMinimum(1)
        self.availTchCountSpinBox.setValue(67)
        self.SSIFilterSpinBox = QtGui.QDoubleSpinBox()
        self.SSIFilterSpinBox.setMaximum(5.0)
        self.SSIFilterSpinBox.setMinimum(0.1)
        self.SSIFilterSpinBox.setValue(1.0)

        glayout.addWidget(self.iRxlevCellSpinBox,0,1)
        glayout.addWidget(QtGui.QLabel('dB'),0,2)
        glayout.addWidget(self.dRxlevCellSpinBox,1,1)
        glayout.addWidget(QtGui.QLabel('dB'),1,2)
        glayout.addWidget(self.aRxlevCellSpinBox,2,1)
        glayout.addWidget(QtGui.QLabel('dB'),2,2)
        glayout.addWidget(self.iRxlevSSISpinBox,3,1)
        glayout.addWidget(QtGui.QLabel('dB'),3,2)
        glayout.addWidget(self.availTchCountSpinBox,4,1)
        glayout.addWidget(self.SSIFilterSpinBox,5,1)
        #glayout.addWidget(QtGui.QLabel('dB'),4,2)
        frame = QtGui.QFrame()
        frame.setLayout(glayout)
        frame.setFrameShape(QtGui.QFrame.StyledPanel)
        hlayour = QtGui.QVBoxLayout()
        hlayour.addWidget(frame)
        DBB = QtGui.QDialogButtonBox(QtGui.QDialogButtonBox.Ok|QtGui.QDialogButtonBox.Cancel)
        hlayour.addWidget(DBB)
        self.setLayout(hlayour)
        self.setWindowTitle('Generate cell coverage report.')

        self.connect(DBB,QtCore.SIGNAL('accepted()'),self,QtCore.SLOT('accept()'))
        self.connect(DBB,QtCore.SIGNAL('rejected()'),self,QtCore.SLOT('reject()'))


class DataManagerDialog(QtGui.QDialog):
    def __init__(self,parent = None,MMLPath = '',MRFiles = (),TEMSLogs = (),GSMCellFile = '',GSMKPIFile='',GSMTrafficFile=''):
        super(DataManagerDialog,self).__init__(parent)
        self.MMLPath = None
        self.MRFiles = []
        self.TEMSLogs = []
        self.GSMCellFile = None
        self.GSMKPIFile = None
        self.GSMTrafficFile = None

        self.setpLabel = QtGui.QLabel()
        self.setpLabel.setText(QtCore.QString('<b>Select data to load</b>'))
        self.setpLabel.setAlignment(QtCore.Qt.AlignLeft|QtCore.Qt.AlignTop)

        self.actionFrame = QtGui.QFrame()
        self.actionFrame.setFrameStyle(QtGui.QFrame.Panel)

        GSMCellInfoLabel = QtGui.QLabel('GSM cell information')
        self.GSMCellInfoEdit= QtGui.QLineEdit()
        self.GSMCellInfoEdit.setText(GSMCellFile)
        self.GSMCellInfoLoadChecker = QtGui.QCheckBox()
        GSMCellInfoLabel.setBuddy(self.GSMCellInfoLoadChecker)
        self.GSMCellInfoLoadChecker.setToolTip('Click to select file and mark to be load')
        self.connect(self.GSMCellInfoLoadChecker,QtCore.SIGNAL('clicked(bool)'),self.selectGSMCellInfo)

        MMLPathLabel = QtGui.QLabel('Huawei MML directory')
        self.MMLPathEdit=  QtGui.QLineEdit()
        self.MMLPathEdit.setText(MMLPath)
        self.MMLLoadChecker = QtGui.QCheckBox()
        MMLPathLabel.setBuddy(self.MMLLoadChecker)
        self.MMLLoadChecker.setToolTip('Click to select directory and mark to be load')
        self.connect(self.MMLLoadChecker,QtCore.SIGNAL('clicked(bool)'),self.selectMMLPath)

        GSMMRLabel = QtGui.QLabel('GSM MR files')
        self.GsmMrList = QtGui.QListWidget()
        self.GsmMrList.addItems(MRFiles)
        self.GsmMrList.setFixedHeight(80)
        self.GsmMrLoadChecker = QtGui.QCheckBox()
        GSMMRLabel.setBuddy(self.GsmMrLoadChecker)
        self.GsmMrLoadChecker.setToolTip('Click to select files and mark to be load')
        self.connect(self.GsmMrLoadChecker,QtCore.SIGNAL('clicked(bool)'),self.selectGsmMRs)

        GSMTrafficLabel = QtGui.QLabel('GSM traffic distribution sample')
        self.GSMTrafficEdit = QtGui.QLineEdit()
        self.GSMTrafficEdit.setText(GSMTrafficFile)
        self.GSMTrafficLoadChecker = QtGui.QCheckBox()
        GSMTrafficLabel.setBuddy(self.GSMTrafficLoadChecker)
        self.GSMTrafficLoadChecker.setToolTip('Click to select file and mark to be load')
        self.connect(self.GSMTrafficLoadChecker,QtCore.SIGNAL('clicked(bool)'),self.selectGsmTraffic)

        GSMKpiLabel = QtGui.QLabel('GSM KPIs')
        self.GSMKpiEdit = QtGui.QLineEdit()
        self.GSMKpiEdit.setText(GSMKPIFile)
        self.GSMKpiLoadChecker = QtGui.QCheckBox()
        GSMKpiLabel.setBuddy(self.GSMKpiLoadChecker)
        self.GSMKpiLoadChecker.setToolTip('Click to select file and mark to be load')
        self.connect(self.GSMKpiLoadChecker,QtCore.SIGNAL('clicked(bool)'),self.selectGsmKpis)

        TemsFmtLabel = QtGui.QLabel('TEMS converted FMT logs')
        self.TemsFmtList = QtGui.QListWidget()
        self.TemsFmtList.addItems(TEMSLogs)
        self.TemsFmtList.setFixedHeight(80)
        self.TemsFmtLoadChecker = QtGui.QCheckBox()
        TemsFmtLabel.setBuddy(self.TemsFmtLoadChecker)
        self.TemsFmtLoadChecker.setToolTip('Click to select files and mark to be load')
        self.connect(self.TemsFmtLoadChecker,QtCore.SIGNAL('clicked(bool)'),self.selectTemsFmts)

        gridLayout = QtGui.QGridLayout()

        row = gridLayout.rowCount()
        gridLayout.addWidget(GSMCellInfoLabel,row,0)
        gridLayout.addWidget(self.GSMCellInfoEdit,row,1)
        gridLayout.addWidget(self.GSMCellInfoLoadChecker,row,2)

        row = gridLayout.rowCount()
        gridLayout.addWidget(GSMKpiLabel,row,0)
        gridLayout.addWidget(self.GSMKpiEdit,row,1)
        gridLayout.addWidget(self.GSMKpiLoadChecker,row,2)

        row = gridLayout.rowCount()
        gridLayout.addWidget(GSMTrafficLabel,row,0)
        gridLayout.addWidget(self.GSMTrafficEdit,row,1)
        gridLayout.addWidget(self.GSMTrafficLoadChecker,row,2)

        row = gridLayout.rowCount()
        gridLayout.addWidget(MMLPathLabel,row,0)
        gridLayout.addWidget(self.MMLPathEdit,row,1)
        gridLayout.addWidget(self.MMLLoadChecker,row,2)

        row = gridLayout.rowCount()
        gridLayout.addWidget(GSMMRLabel,row,0)
        gridLayout.addWidget(self.GsmMrList,row,1)
        gridLayout.addWidget(self.GsmMrLoadChecker,row,2)

        row = gridLayout.rowCount()
        gridLayout.addWidget(TemsFmtLabel,row,0)
        gridLayout.addWidget(self.TemsFmtList,row,1)
        gridLayout.addWidget(self.TemsFmtLoadChecker,row,2)

        self.actionFrame.setLayout(gridLayout)
        vLayout = QtGui.QVBoxLayout()
        vLayout.addWidget(self.setpLabel)
        vLayout.addWidget(self.actionFrame)
        loadButton = QtGui.QPushButton('&Load')
        closeButton = QtGui.QPushButton('&Close')
        self.connect(closeButton,QtCore.SIGNAL('clicked()'),self,QtCore.SLOT('close()'))
        self.connect(loadButton,QtCore.SIGNAL('clicked()'),self.loadData)

        hLayout = QtGui.QHBoxLayout()
        hLayout.addWidget(loadButton)
        hLayout.addWidget(closeButton)
        vLayout.addLayout(hLayout)

        self.setLayout(vLayout)
        self.setWindowTitle('Data manager')

    def loadData(self):
        dataSet = dict(MmlPath = self.MMLPath if self.MMLLoadChecker.isChecked() else None,
            MrFiles = self.MRFiles if self.GsmMrLoadChecker.isChecked() else [],
            TemsLogs = self.TEMSLogs if self.TemsFmtLoadChecker.isChecked() else [],
            GSMCellFile = self.GSMCellFile if self.GSMCellInfoLoadChecker.isChecked() else None,
            GSMKPIFile = self.GSMKPIFile if self.GSMKpiLoadChecker.isChecked() else None,
            GSMTrafficFile = self.GSMTrafficFile if self.GSMTrafficLoadChecker.isChecked() else None
        )

        self.emit(QtCore.SIGNAL('loadRequest'),dataSet)
        self.close()

    def selectMMLPath(self,checked):
        if checked:
            selectPath = QtGui.QFileDialog.getExistingDirectory()
            if selectPath:
                self.MMLPathEdit.setText(selectPath)
                self.MMLPath = unicode(selectPath)
            else:
                if not self.MMLPathEdit.text():
                    self.MMLLoadChecker.setChecked(False)

    def selectGSMCellInfo(self,checked):
        if checked:
            selected = QtGui.QFileDialog.getOpenFileName(self,filter = 'CSV Files (*.csv)')
            if selected:
                self.GSMCellInfoEdit.setText(selected)
                self.GSMCellFile = unicode(selected)
            else:
                if not self.GSMCellInfoEdit.text():
                    self.GSMCellInfoLoadChecker.setChecked(False)

    def selectGsmMRs(self,checked):
        if checked:
            selectFiles = QtGui.QFileDialog.getOpenFileNames(self,filter = 'CSV Files (*.csv)')
            if selectFiles:
                while self.GsmMrList.takeItem(0):
                    pass
                for fileName in selectFiles:
                    self.GsmMrList.addItem(fileName)
                    self.MRFiles.append(unicode(fileName))
            else:
                if 0 == self.GsmMrList.count() :
                    self.GsmMrLoadChecker.setChecked(False)

    def selectGsmTraffic(self,checked):
        if checked:
            selected = QtGui.QFileDialog.getOpenFileName(self,filter = 'CSV Files (*.csv)')
            if selected:
                self.GSMTrafficEdit.setText(selected)
                self.GSMTrafficFile = unicode(selected)
            else:
                if not self.GSMTrafficEdit.text():
                    self.GSMTrafficEditChecker.setChecked(False)

    def selectGsmKpis(self,checked):
        if checked:
            selected = QtGui.QFileDialog.getOpenFileName(self,filter = 'CSV Files (*.csv)')
            if selected:
                self.GSMKpiEdit.setText(selected)
                self.GSMKPIFile = unicode(selected)
            else:
                if not self.GSMKpiEdit.text():
                    self.GSMKpiLoadChecker.setChecked(False)

    def selectTemsFmts(self,checked):
        if checked:
            selectFiles = QtGui.QFileDialog.getOpenFileNames(self,filter = 'Tems FMT log Files (*.fmt)')
            if selectFiles:
                while self.TemsFmtList.takeItem(0):
                    pass
                for fileName in selectFiles:
                    self.TemsFmtList.addItem(fileName)
                    self.TEMSLogs.append(unicode(fileName))
            else:
                if 0 == self.TemsFmtList.count() :
                    self.TemsFmtLoadChecker.setChecked(False)

class OSMBackgroundLabor(QtCore.QThread):
    fetchCompleteSignal = QtCore.pyqtSignal(str,tuple,float,int)

    def __init__(self,gisView,workingPath = './OpenStreetMap',parent = None):
        super(OSMBackgroundLabor,self).__init__(parent)
        self.logger = logging.getLogger('global')
        self.proj = _globals.getViewerProj()
        self.view = gisView
        self.workingDir = None
        self.filePatten = 'osm.{}.{}.{}.png'
        self.fetchUrl = 'http://tile.openstreetmap.org/{}/{}/{}.png'
        self.missionIncome = QtCore.QWaitCondition()
        self.missionIncomeMutex = QtCore.QMutex()
        self.missionIncomeMutex.lock()
        if os.path.exists(os.path.abspath(workingPath)):
            self.workingDir = workingPath
        else:
            try:
                os.mkdir(os.path.abspath(workingPath))
                if os.path.exists(os.path.abspath(workingPath)):
                    self.workingDir = workingPath
                else:
                    raise IOError,'Failed to create path {}'.format(os.path.abspath(workingPath))
            except:
                self.logger.exception('Failed to create path {}'.format(os.path.abspath(workingPath)))

    def run(self):
        while True:
            #self.logger.debug('Waiting for new load mission...')
            self.missionIncome.wait(self.missionIncomeMutex)
            #self.logger.debug('New map download mission triggered.')
            zoomLevel,startTile,maxTiles = self.view.getRequiredZoomLevel()
            if zoomLevel and self.workingDir:
                for x in range(startTile[0],startTile[0]+ maxTiles+1):
                    for y in range(startTile[1],startTile[1]+ maxTiles+1):
                        if zoomLevel != self.view.zoomLevel:
                            break
                        fileToShow = os.path.join(os.path.abspath(self.workingDir),self.filePatten).format(zoomLevel,x,y)
                        #self.logger.debug('File {} rquired.'.format(fileToShow))
                        try:
                            if not os.path.exists(fileToShow):
                                #self.logger.debug('File {} download required.'.format(fileToShow))
                                fp = urllib2.urlopen(self.fetchUrl.format(zoomLevel,x,y))
                                if fp.geturl() == self.fetchUrl.format(zoomLevel,x,y):
                                    with open(fileToShow,'wb') as tmp:
                                        tmp.write(fp.read())
                                    #self.logger.debug('File {} download success.'.format(fileToShow))
                                else:
                                    self.logger.debug('Failed to fetch {}'.format(zoomLevel,x,y))
                            else:
                                #self.logger.debug('File {} exist,download avoided.'.format(fileToShow))
                                pass
                        except:
                            self.logger.debug('Failed to fetch {}'.format(zoomLevel,x,y))

                        #self.logger.debug('Iam herecccc')
                        if os.path.exists(fileToShow):
                            if zoomLevel == self.view.zoomLevel:
                                #self.logger.debug('Iam stuck here')
                                tileCoords = self.proj(*self.view.num2deg(x,y,zoomLevel))
                                tileEndCoords = self.proj(*self.view.num2deg(x+1,y+1,zoomLevel))
                                #self.logger.debug('Iam not here')
                                pos = (tileCoords[0],-1*tileCoords[1])

                                scale = (tileEndCoords[0] - tileCoords[0])/self.view.TDIM
                                #self.logger.debug('Shooting information:{},{},{},{}'.format(fileToShow,pos,scale,zoomLevel))
                                self.fetchCompleteSignal.emit(fileToShow,pos,scale,zoomLevel)
                                #self.logger.debug('File {} load and shooted.'.format(fileToShow))
                            else:
                                #self.logger.debug('Zoom leve miss match,file level {},view level {}.'.format(zoomLevel,self.view.zoomLevel))
                                break
                        else:
                             self.logger.debug('Escaping load {},file missing.'.format(fileToShow))
            else:
                self.logger.debug('No zoom info or working dir.')
        self.logger.debug('Map load thread completed.')


class CalcBackgroundLabor(QtCore.QThread):
    taskCompleteSignal = QtCore.pyqtSignal(str)

    def __init__(self,parent = None):
        super(CalcBackgroundLabor,self).__init__(parent)
        self.missionIncome = QtCore.QWaitCondition()
        self.missionIncomeMutex = QtCore.QMutex()
        self.missionIncomeMutex.lock()
        self.missionDone = QtCore.QWaitCondition()
        self.missionDoneMutex = QtCore.QMutex()
        self.missionDoneMutex.lock()
        self.__runnerAgrs = tuple()
        self.__runnerKwArgs = dict()
        self.__runner = None
        self.__runnable = True
        self.runnerResult = None
        self.runnerError = False
        self.logger = logging.getLogger('global')

    def run(self):
        while self.__runnable:
            self.missionIncome.wait(self.missionIncomeMutex)
            #self.logger.debug('New task coming,background func {},with arg {} and kwarg {}'.format(self.__runner,self.__runnerAgrs,self.__runnerKwArgs))
            if self.__runner:
                try:
                    #self.logger.debug('Running background func {},with arg {} and kwarg {}'.format(self.__runner,self.__runnerAgrs,self.__runnerKwArgs))
                    self.runnerError = False
                    self.runnerResult = None
                    self.runnerResult = self.__runner(*self.__runnerAgrs,**self.__runnerKwArgs)
                    try:
                        self.taskCompleteSignal.emit(self.__runner.__doc__)
                    except TypeError:
                        pass
                except:
                    self.logger.exception('Unexpected error found in runner thread!')
                    self.runnerError = True
                finally:
                    self.__runner = None
                    self.__runnerAgrs = tuple()
                    self.__runnerKwArgs = dict()
                    self.missionDone.wakeAll()

        self.logger.debug('Background runner exited...')

    def __shootToReturn(self,runner,*args,**kwargs):
        if runner:
            #self.logger.debug('Shoot to return...')
            self.__runner = runner
            if args:
                self.__runnerAgrs = args
            if kwargs:
                self.__runnerKwArgs = kwargs
            self.missionIncome.wakeAll()

    def __shootToWait(self,runner,*args,**kwargs):
        #self.logger.debug('Shoot to wait...')
        self.__shootToReturn(runner,*args,**kwargs)
        self.missionDone.wait(self.missionDoneMutex)

    def shoot(self,blocking,runner,*args,**kwargs):
        #self.logger.debug('Blocking:{},runner:{},arg {},kwarg {}'.format(blocking,runner,args,kwargs))
        if blocking:
            self.__shootToWait(runner,*args,**kwargs)
        else:
            self.__shootToReturn(runner,*args,**kwargs)

    def terminate(self):
        self.__runnable = False
        super(CalcBackgroundLabor,self).terminate()

class MainWindow(QtGui.QMainWindow):

    def __init__(self,parent = None,controller = None):
        super(MainWindow,self).__init__(parent)
        self.logger = logging.getLogger('global')
        self.controller = controller
        self.controller.View = self
        self.realProj = _globals.getCalcProj()
        self.mapProj = _globals.getViewerProj()

        self.__createLogDockWidget()
        self.__createBackgroundLabors()
        self.__createMenu()
        self.__createGISComponent()

        self.__createCellInfoDockWidget()
        self.__createTemsScanSpotInfoDockWidget()
        self.__createCellCoverageDockWidget()
        self.__createClusterViewDockWidget()
        self.__createBlackMRAnalyzerDockWidget()

        self.ScanCoverageCriterionDockWidget = QtGui.QDockWidget('Scan coverage criterion')
        self.ScanCoverageCriterionDockWidget.setObjectName('SCoverageCriterion')
        self.ScanCoverageCriterionDockWidget.setWidget(ScanCoverageCriterionWidget(self))
        self.addDockWidget(QtCore.Qt.LeftDockWidgetArea,self.ScanCoverageCriterionDockWidget)

        self.StreetCoverageInfoDockWidget = QtGui.QDockWidget('Street coverage info')
        self.StreetCoverageInfoDockWidget.setObjectName('SCoverageInfo')
        self.StreetCoverageInfoDockWidget.setWidget(ScanedSSIInfoWidget(self))
        self.StreetCoverageInfoDockWidget.hide()
        self.addDockWidget(QtCore.Qt.LeftDockWidgetArea,self.StreetCoverageInfoDockWidget)

        self.tabifyDockWidget(self.logDockWidget,self.CellCoverageInfomationDockWidget)

        self.__createStatusBar()
        self.__createCellFinderToolBar()

        AnalysisToolBar = self.addToolBar('Analysis')
        resetMapsBT = QtGui.QPushButton('Reset Maps')
        self.connect(resetMapsBT,QtCore.SIGNAL('clicked(bool)'),self.resetMapStatus)
        AnalysisToolBar.addWidget(resetMapsBT)

        self.setWindowTitle('Network structral analyzer')

        self.__initActions()
        self.setCentralWidget(self.gisViewer)

        self.workingLabor.taskCompleteSignal.connect(self.onBackgroundTaskComplete)
        self.gisViewer.itemDetectedSignal.connect(self.onGraphicsItemDetect)
        self.messageQueueThread.messageIncomeEvent.connect(self.infoWidget.log)
        self.progressQueueThread.progressEvent.connect(self.runningProgressBar.setValue)

        self.workingLabor.start()
        self.messageQueueThread.start()
        self.progressQueueThread.start()
        self.logger.debug('I am a tester!')

    def __createBlackMRAnalyzerDockWidget(self):
        self.blackMRAnalyzerDockWidget = QtGui.QDockWidget('Black MR Viewer')
        self.blackMRAnalyzerDockWidget.setObjectName('BlackMR')
        self.blackMRAnalyzerTable = QtGui.QTableWidget(self)
        self.blackMRAnalyzerTable.setColumnCount(5)
        self.blackMRAnalyzerTable.setRowCount(5)
        self.blackMRAnalyzerTable.setHorizontalHeaderLabels(['BlackMRID','ReportCells','bMRs','rMRs','bRatio'])
        self.blackMRAnalyzerTable.resizeColumnsToContents()
        self.blackMRAnalyzerDockWidget.setWidget(self.blackMRAnalyzerTable)
        self.connect(self.blackMRAnalyzerTable,QtCore.SIGNAL('cellClicked(int,int)'),lambda row,column:self.visualizeBlackMRByID( unicode(self.blackMRAnalyzerTable.item(row,0).text()) if self.blackMRAnalyzerTable.item(row,0) else None ) )
        self.addDockWidget(QtCore.Qt.LeftDockWidgetArea,self.blackMRAnalyzerDockWidget)
        self.blackMRAnalyzerDockWidget.hide()


    def __createClusterViewDockWidget(self):
        self.clusterViewerDockWidget = QtGui.QDockWidget('Cluster Viewer')
        self.clusterViewerDockWidget.setObjectName('ClusterViewer')
        self.clusterViewerTable = QtGui.QTableWidget(self)
        self.clusterViewerTable.setColumnCount(5)
        self.clusterViewerTable.setHorizontalHeaderLabels([u'簇ID',u'小区数',u'载波数',u'同频频点数',u'同频载波数'])
        self.clusterViewerTable.resizeColumnsToContents()
        self.clusterViewerTable.resizeRowsToContents()
        self.clusterViewerTable.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        #self.clusterViewerTable.setSortingEnabled(True)
        self.clusterViewerDockWidget.setWidget(self.clusterViewerTable)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea,self.clusterViewerDockWidget)
        self.clusterViewerDockWidget.hide()

        self.connect(self.clusterViewerTable,QtCore.SIGNAL('cellClicked(int,int)'),lambda row,column:self.showCellsByClusterID(self.clusterViewerTable.item(row,0).value()))
        self.connect(self.clusterViewerTable,QtCore.SIGNAL('customContextMenuRequested(QPoint)'),self.onContextMenuForClusterViewer)

    def __createLogDockWidget(self):
        self.logDockWidget = QtGui.QDockWidget("What's going on?",self)
        self.logDockWidget.setObjectName('logDock')
        self.logDockWidget.setAllowedAreas(QtCore.Qt.BottomDockWidgetArea|QtCore.Qt.TopDockWidgetArea)
        self.infoWidget = LogInfoListWidget(self)
        self.logDockWidget.setWidget(self.infoWidget)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea,self.logDockWidget)

    def __createBackgroundLabors(self):
        self.messageQueueThread = MessageQueueThread(self)
        self.progressQueueThread = ProgressQueueThread(self)
        self.workingLabor = CalcBackgroundLabor(self)

    def __createGISComponent(self):
        self.gisViewer = GISGraphicView(self)
        self.gisScene = GISScene(self)
        self.gisViewer.setScene(self.gisScene)
        self.gisViewer.CursorLocationSignal.connect(self.updateCursorLocation)
        self.gisViewer.CellRMenuRequestSignal.connect(self.popupCellRMenu)

    def __createCellFinderToolBar(self):
        CellFinderToolBar = self.addToolBar('Cell Finder')
        CellFinderToolBar.addWidget(QtGui.QLabel('CGI to Find:'))
        self.cgiToFindEdit = QtGui.QLineEdit()
        self.cgiAutoCompleter = QtGui.QCompleter(self.cgiToFindEdit)
        self.cgiToFindEdit.setCompleter(self.cgiAutoCompleter)
        self.cgiToFindEdit.setMinimumWidth(200)
        CellFinderToolBar.addWidget(self.cgiToFindEdit)
        bt = QtGui.QPushButton('Find')
        CellFinderToolBar.addWidget(bt)
        #self.addToolBarBreak()
        self.connect(self.cgiToFindEdit,QtCore.SIGNAL('returnPressed()'),self.onFindCellHandler)
        self.connect(bt,QtCore.SIGNAL('clicked()'),self.onFindCellHandler)

    def __createMenu(self):
        self.projectMenu = self.menuBar().addMenu('&Project')
        self.ToolsMenu = self.menuBar().addMenu('&Tools')
        self.scanMenu = self.menuBar().addMenu('&Scan App')
        self.mrMenu = self.menuBar().addMenu('&MR App')
        self.reportMenu = self.menuBar().addMenu('&Report')
        self.helpMenu = self.menuBar().addMenu('&Help')
        self.LogicalItemPopupMenu = QtGui.QMenu(self)
        self.clusterViewerPopupMenu = QtGui.QMenu(self)

    def __createStatusBar(self):
        statusBar = QtGui.QStatusBar()
        self.runningProgressBar = QtGui.QProgressBar()
        self.runningProgressBar.setMinimum(0)
        self.runningProgressBar.setMaximum(100)
        self.runningProgressBar.setValue(0)
        self.cursorLocationLabel = QtGui.QLabel()
        self.cursorLocationLabel.setFrameStyle(QtGui.QFrame.StyledPanel|QtGui.QFrame.Sunken)
        self.cursorLocationLabel.setMinimumWidth(150)
        statusBar.addPermanentWidget(self.runningProgressBar)
        statusBar.addPermanentWidget(self.cursorLocationLabel)
        self.messageQueueThread.messageIncomeEvent.connect(statusBar.showMessage)
        self.setStatusBar(statusBar)

    def __createCellInfoDockWidget(self):
        self.CellInfoDockWidget = QtGui.QDockWidget('Cell Info')
        self.CellInfoDockWidget.setObjectName('CellInfo')
        self.CellInfoTable = QtGui.QTableWidget(self)
        self.tableHeaders = [u'BSC',u'CGI',u'CellName',u'BCCH',u'BSIC',u'TCH频点',u'天线挂高',u'天线下倾',u'天线方向',u'覆盖类型',u'测量报告/6',u'覆盖小区数',u'过覆盖小区数',u'同频小区对',u'相关连通簇',u'问题连通簇']
        self.CellInfoTable.setColumnCount(1)
        self.CellInfoTable.setRowCount(len(self.tableHeaders))
        self.CellInfoTable.setVerticalHeaderLabels(self.tableHeaders)
        self.CellInfoTable.setHorizontalHeaderLabels(['Value'])
        self.CellInfoTable.setColumnWidth(0,150)
        self.CellInfoDockWidget.setWidget(self.CellInfoTable)
        #self.CellInfoWidget = CellInfoWidget(self
        #self.CellInfoDockWidget.setWidget(self.CellInfoWidget)
        self.addDockWidget(QtCore.Qt.LeftDockWidgetArea,self.CellInfoDockWidget)

    def updateCellInfoTable(self,attrs):
        for i in range(len(self.tableHeaders)):
            self.CellInfoTable.takeItem(i,0)
        if attrs:
            for key in attrs:
                if key == 'cgi':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'CGI'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'bcch':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'BCCH'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'dir':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'天线方向'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'type':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'覆盖类型'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'name':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'CellName'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'bsic':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'BSIC'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'tile':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'天线下倾'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'height':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'天线挂高'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'mrs':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'测量报告/6'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'covered':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'覆盖小区数'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'overlapped':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'过覆盖小区数'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'coArfcnCells':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'同频小区对'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'cluster':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'相关连通簇'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'pcluster':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'问题连通簇'),0,QtGui.QTableWidgetItem('{}'.format(attrs[key])))
                elif key == 'tchs':
                    self.CellInfoTable.setItem(self.tableHeaders.index(u'TCH频点'),0,QtGui.QTableWidgetItem(' '.join(['{}'.format(item) for item in attrs[key]])))

    def __createTemsScanSpotInfoDockWidget(self):
        self.ScanInfoDockWidget = QtGui.QDockWidget('TEMS Scan sample',self)
        self.ScanInfoDockWidget.setObjectName('TemsScanDock')
        self.ScanInfoTable = QtGui.QTableWidget(self)
        self.ScanInfoTable.setColumnCount(8)
        self.ScanInfoTable.setHorizontalHeaderLabels(['CGI','ARFCN','BSIC','AvgRxlev','Samples','Distance','Reference sample distance','TCH Counts'])
        self.ScanInfoTable.resizeColumnsToContents()
        self.ScanInfoTable.resizeRowsToContents()
        self.ScanInfoTable.setSortingEnabled(True)
        self.ScanInfoDockWidget.setWidget(self.ScanInfoTable)
        self.addDockWidget(QtCore.Qt.LeftDockWidgetArea,self.ScanInfoDockWidget)
        self.ScanInfoDockWidget.hide()

    def __createCellCoverageDockWidget(self):
        self.CellCoverageInfomationDockWidget = QtGui.QDockWidget('Cell coverage statistics')
        self.CellCoverageInfomationDockWidget.setObjectName('CellCoverageDock')
        self.CellCoverageInfomationTable = QtGui.QTableWidget(self)
        self.CellCoverageInfomationTable.setColumnCount(18)
        self.CellCoverageInfomationTable.setHorizontalHeaderLabels(['CGI',] + self.controller.cellCoverageResultDictIndex + ['Dominate/(Dominate + Interfer)','OverlappedDominate/Dominate','Overlapped/(Dominate + Interfer)'])
        self.CellCoverageInfomationTable.resizeColumnsToContents()
        self.CellCoverageInfomationTable.setSortingEnabled(True)
        self.CellCoverageInfomationDockWidget.setWidget(self.CellCoverageInfomationTable)
        self.addDockWidget(QtCore.Qt.BottomDockWidgetArea,self.CellCoverageInfomationDockWidget)
        self.CellCoverageInfomationDockWidget.hide()
        self.connect(self.CellCoverageInfomationTable,QtCore.SIGNAL('cellClicked(int,int)'),self.locateCellFromCellCoverageTable)

    def __initActions(self):
        self.createProjectAction = self.createAction('&New ...',self.newProjectGuideHandler)
        self.openProjectAction = self.createAction('&Open ...',self.openProjectHandler)
        self.closeProjectAction = self.createAction('&Close',self.close)
        self.saveAsProjectAction = self.createAction('&Save As ...',self.saveAsProjectHandler)
        self.exitAction = self.createAction('&Exit',self.close)

        self.LoadDataAction = self.createAction('Data &Manager...',self.dataManagerDispatcher)
        self.ObjectTraceAction = self.createAction('GIS object auto trace',self.toggleObjectTraceMode,QtCore.Qt.Key_T,checkable=True)
        self.enableMapBackgroundAction = self.createAction('Enable OpenStreetMap',self.toggleBackgroundMap,checkable = True)

        self.DrawSiteAction = self.createAction('Plot &GSM Cell',self.drawSiteView)
        self.DrawFmtScanSampleAction  = self.createAction('Plot &TEMS scan sample',self.drawFMTScanLog)
        self.DrawFmtScanSampleBySsiAction  = self.createAction('Calc and Plot &SSI TEMS scan sample',self.CalcDrawTemsScanLogBySSI)
        self.ScanCellCoverageAnalysisAction = self.createAction('Cell coverage analysis (TEMS Scan)',self.onTemsScanCellCoverageAction,QtCore.Qt.Key_C,checkable=True)
        self.ScanSpotCoverageAnalysisAction = self.createAction('Spot coverage analysis (TEMS Scan)',self.onTemsScanSpotCoverageAction,QtCore.Qt.Key_S,checkable=True)
        self.GenerateCellCoverageReportBySSIAction  = self.createAction('Generate Cell Coverage Report by &Scan Data...',self.generateCellCoverageReportByScanData)
        self.CalcCellCoverageReportBySSIAction  = self.createAction('Calcuate Cell Coverage statistics by &Scan Data...',self.calcCellCoverageStatisticsByScanData)

        self.genMaximalConnecedtClusterReportAction = self.createAction('Generate Maximal Connected Cluster report...',self.genMaximalConnecedtClusterReport)
        self.calcMaximalConnecedtClusterAction = self.createAction('Calculate Maximal Connected Cluster',self.calcMaximalConnectedCluster)
        self.genGridMapAction = self.createAction('Generate raster KPI Map...',self.generateGridMap)
        self.reFreqPlanAction = self.createAction('RF RePlaning...',self.showRFRePlanDialog)

        self.showCoveredNeisAction = self.createAction('Covered cells by 12dB>3%',self.showCoveredNeis)
        self.showOverlappedNeisAction = self.createAction('Overlapped cells by 12dB>3% && Geo',self.showOverlappedNeis)
        #self.showMaxConnectedCellsAction = self.createAction('Max connected cluster')
        self.showRelatedClustersAction = self.createAction('Maximal connected clusters',self.showRelatedClusters)
        self.showConflictCellsByCoArfcnAction = self.createAction('Conflict cells by Co-Arfcn',self.showConflictCellsByCoArfcn)

        self.showGlobalProblemClustersAction = self.createAction('Calculate global problem cluster',self.showGlobalProblemClusters)
        self.showGlobalCoArfcnPairAction = self.createAction('Calculate global co-Arfcn pairs...',self.showGlobalCoArfcnPairs)

        self.showSelectedClustersAction = self.createAction('Show selected clusters',self.showSelectedClusters)

        self.analyzeBlackMRAction = self.createAction('Visualize Black MR',self.analyzeBlackMR)

        self.addActions(self.projectMenu,(self.createProjectAction,self.openProjectAction,self.saveAsProjectAction,self.closeProjectAction,None,self.exitAction))
        self.addActions(self.scanMenu,(self.DrawFmtScanSampleAction,self.DrawFmtScanSampleBySsiAction,None,self.ScanSpotCoverageAnalysisAction,self.ScanCellCoverageAnalysisAction,None,self.CalcCellCoverageReportBySSIAction))
        self.addActions(self.mrMenu,(self.calcMaximalConnecedtClusterAction,None,self.showGlobalCoArfcnPairAction,self.showGlobalProblemClustersAction,self.analyzeBlackMRAction,None,self.reFreqPlanAction))
        self.addActions(self.ToolsMenu,(self.LoadDataAction,None,self.enableMapBackgroundAction,self.DrawSiteAction,None,self.ObjectTraceAction))
        self.addActions(self.reportMenu,(self.GenerateCellCoverageReportBySSIAction,None,self.genMaximalConnecedtClusterReportAction,self.genGridMapAction))
        self.addActions(self.LogicalItemPopupMenu,(self.showCoveredNeisAction,self.showOverlappedNeisAction,self.showConflictCellsByCoArfcnAction,None,self.showRelatedClustersAction))
        self.addActions(self.clusterViewerPopupMenu,(self.showSelectedClustersAction,))

    def updateCursorLocation(self,coords):
            self.cursorLocationLabel.setText('{}'.format(coords))

    def popupCellRMenu(self,MouseEvent,Item):
        self.LogicalItemPopupMenu.focusedCell = Item.logicalId
        self.LogicalItemPopupMenu.popup(MouseEvent.globalPos())

    def showRFRePlanDialog(self):
        self.planOptionDialog = RFReplanOptionDialog(self)
        self.planOptionDialog.selectOnGISRequestSignal.connect(self.enterPolygonSelectionMode)
        self.planOptionDialog.startRePlanRequestSignal.connect(self.runConflictingTRXResolver)
        self.planOptionDialog.show()

    def analyzeBlackMR(self,minmumCells = 3 ,mrRatio = 0.1):
        try:
            result = None
            self.workingLabor.shoot(True,self.controller.getBlackMRList,minmumCells,mrRatio)
            if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                result = self.workingLabor.runnerResult
            #result = self.controller.getBlackMRList(minmumCells,mrRatio)
            if result:
                for removeIdx in range(self.blackMRAnalyzerTable.rowCount()):
                    self.blackMRAnalyzerTable.removeRow(removeIdx)
                self.blackMRAnalyzerTable.setRowCount(len(result))
                rowCount = 0
                for row in result:
                    columnCount = 0
                    for value in row:
                        #self.logger.debug('Insert at {},{} value {}'.format(rowCount,columnCount,value))
                        self.blackMRAnalyzerTable.setItem(rowCount,columnCount,QtGui.QTableWidgetItem('{}'.format(value)))
                        columnCount += 1
                    rowCount += 1
            self.blackMRAnalyzerDockWidget.show()
            self.blackMRAnalyzerDockWidget.raise_()
        except:
            self.logger.exception('Unexpected error found when analyze Balck MRs')

    def visualizeBlackMRByID(self,blackID ,mrRatio = 0.1):
        if not blackID:
            return
        self.logger.info('To be visualized blackMR ID:{}'.format(blackID))
        self.gisScene.clearCellTraceLine()
        try:
            result = None
            self.workingLabor.shoot(True,self.controller.getBlackMrAffectCells,blackID,mrRatio)
            if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                result = self.workingLabor.runnerResult
            #result = self.controller.getBlackMrAffectCells(blackID ,mrRatio)
            #self.logger.debug('Got {}'.format(result))
            if result:
                pre = None
                for row in result:
                    if row[1] in self.gisScene.GraphicsItemLogicalMapping['cgi']:
                        if pre:
                            item = self.gisScene.GraphicsItemLogicalMapping['cgi'].get(pre)
                            self.__drawLineToTransmitter(item.sceneBoundingRect().center(),row[1],COARFCN_PEN)
                        pre = row[1]
            self.gisViewer.centerOn(self.gisScene.GraphicsItemLogicalMapping['cgi'].get(pre))
        except:
            self.logger.exception('Unexpected error detected when visualize black MR {}'.format(blackID))

    def showCoveredNeis(self):
        self.logger.debug('Show covered cells for cgi {} required'.format(self.LogicalItemPopupMenu.focusedCell))
        self.gisScene.toggleGraphicItemHighlightStatus([])
        #self.gisScene.clearCellTraceLine()
        result = None
        self.workingLabor.shoot(True,self.controller.getCoveredNeisByHist,self.LogicalItemPopupMenu.focusedCell)
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            result = self.workingLabor.runnerResult
        #result = self.controller.getCoveredNeisByHist(self.LogicalItemPopupMenu.focusedCell)
        if result:
            toogleItems = [self.gisScene.GraphicsItemLogicalMapping['cgi'].get(cgi) for cgi in result if self.gisScene.GraphicsItemLogicalMapping['cgi'].get(cgi)]
            self.gisScene.toggleGraphicItemHighlightStatus(toogleItems)
#            item = self.gisScene.GraphicsItemLogicalMapping['cgi'].get(self.popupMenu.focusedCell)
#            if item:
#                for row in result:
#                    self.__drawLineToTransmitter(item.sceneBoundingRect().center(),row,COARFCN_PEN)
        self.logger.debug('get {}'.format(result))

    def showOverlappedNeis(self):
        self.logger.debug('Show overlaped cells for cgi {} required'.format(self.LogicalItemPopupMenu.focusedCell))
        self.gisScene.toggleGraphicItemHighlightStatus([])
        #self.gisScene.clearCellTraceLine()
        result = None
        self.workingLabor.shoot(True,self.controller.getOverlayedNeisByHist,self.LogicalItemPopupMenu.focusedCell)
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            result = self.workingLabor.runnerResult
        #result = self.controller.getOverlayedNeisByHist(self.LogicalItemPopupMenu.focusedCell)
        if result:
#            item = self.gisScene.GraphicsItemLogicalMapping['cgi'].get(self.popupMenu.focusedCell)
#            if item:
#                for row in result:
#                    self.__drawLineToTransmitter(item.sceneBoundingRect().center(),row,COARFCN_PEN)
            toogleItems = [self.gisScene.GraphicsItemLogicalMapping['cgi'].get(cgi) for cgi in result if self.gisScene.GraphicsItemLogicalMapping['cgi'].get(cgi)]
            self.gisScene.toggleGraphicItemHighlightStatus(toogleItems)
        self.logger.debug('get {}'.format(result))

    def showConflictCellsByCoArfcn(self):
        self.logger.debug('Show conflict cells for cgi {} required'.format(self.LogicalItemPopupMenu.focusedCell))
        #self.gisScene.toggleGraphicItemHighlightStatus([])
        self.gisScene.clearCellTraceLine()
        result = None
        self.workingLabor.shoot(True,self.controller.getCellConflictArfcnPairs,self.LogicalItemPopupMenu.focusedCell)
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            result = self.workingLabor.runnerResult
        #result = self.controller.getCellConflictArfcnPairs(self.LogicalItemPopupMenu.focusedCell)
        if result:
            item = self.gisScene.GraphicsItemLogicalMapping['cgi'].get(self.LogicalItemPopupMenu.focusedCell)
            if item:
                for row in result:
                    self.__drawLineToTransmitter(item.sceneBoundingRect().center(),row[0],COARFCN_PEN)
        #self.logger.debug('get {}'.format(result))

    def showGlobalProblemClusters(self):
        self.logger.debug('Start fetch global problem clusters...')
        self.gisScene.clearCellTraceLine()
        result = None
        self.workingLabor.shoot(True,self.controller.getGlobalProblemClusters)
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            result = self.workingLabor.runnerResult
        #result = self.controller.getGlobalProblemClusters()
        if result:
            self.logger.debug('Got {},Count {}'.format(result,len(result)))
            #TODO FIX ME setRowCount won't work
            for removeIdx in range(self.clusterViewerTable.rowCount()):
                self.clusterViewerTable.removeRow(removeIdx)
            self.clusterViewerTable.setRowCount(len(result))
            rowCount = 0
            for row in result:
                columnCount = 0
                for value in row:
                    #self.logger.debug('Insert at {},{} value {}'.format(rowCount,columnCount,value))
                    if value != None:
                        self.clusterViewerTable.setItem(rowCount,columnCount,NumberTableWidgetItem(value))
                    else:
                        self.clusterViewerTable.setItem(rowCount,columnCount,NumberTableWidgetItem(0))
                    columnCount += 1
                self.showCellsByClusterID(row[0],False)
                rowCount += 1
        self.clusterViewerDockWidget.show()
        self.clusterViewerDockWidget.raise_()

    def showRelatedClusters(self):
        self.logger.debug('Show clusters for cgi {} required'.format(self.LogicalItemPopupMenu.focusedCell))
        result = None
        self.workingLabor.shoot(True,self.controller.getRelatedClustersByCgi,self.LogicalItemPopupMenu.focusedCell)
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            result = self.workingLabor.runnerResult
        #result = self.controller.getRelatedClustersByCgi(self.LogicalItemPopupMenu.focusedCell)
        if result:
            self.logger.debug('Got {},Count {}'.format(result,len(result)))
            #TODO FIX ME setRowCount won't work
            for removeIdx in range(self.clusterViewerTable.rowCount()):
                self.clusterViewerTable.removeRow(removeIdx)
            self.clusterViewerTable.setRowCount(len(result))
            rowCount = 0
            for row in result:
                columnCount = 0
                for value in row:
                    #self.logger.debug('Insert at {},{} value {}'.format(rowCount,columnCount,value))
                    if value != None:
                        self.clusterViewerTable.setItem(rowCount,columnCount,NumberTableWidgetItem(value))
                    else:
                        self.clusterViewerTable.setItem(rowCount,columnCount,NumberTableWidgetItem(0))
                    columnCount += 1
                rowCount += 1
        self.clusterViewerDockWidget.show()
        self.clusterViewerDockWidget.raise_()

    def showCellsByClusterID(self,clusterId,clearPrevious = True):
        if clearPrevious:
            self.gisScene.clearCellTraceLine()
        if clusterId:
            #self.logger.debug('Show cluster {}'.format(clusterId))
            result = None
            self.workingLabor.shoot(True,self.controller.getCellsByClusterID,clusterId)
            if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                result = self.workingLabor.runnerResult
            #result = self.controller.getCellsByClusterID(clusterId)
            if result:
                for i in range(len(result)):
                    for n in range(i+1,len(result)):
                        item = self.gisScene.GraphicsItemLogicalMapping['cgi'].get(result[i][0])
                        if item != None:
                            self.__drawLineToTransmitter(item.sceneBoundingRect().center(),result[n][0],COARFCN_PEN)

    def showGlobalCoArfcnPairs(self):
        checkType,valid = QtGui.QInputDialog.getItem(self,u'频段类型',u'请选择',[u'900M',u'1800M'],current = 1,editable = False)
        if valid:
            self.gisScene.clearCellTraceLine()
            self.workingLabor.shoot(False,self.controller.getGlobalCoArfcnPairs,BAND = unicode(checkType))

    def onGlobalCoArfcnCheckCompleted(self,result):
        if result:
            bt = QtGui.QMessageBox.question(self,u'同频核查结束',u'是否显示结果',QtGui.QMessageBox.Ok,QtGui.QMessageBox.Cancel)
            if bt == QtGui.QMessageBox.Ok:
                for pair in result:
                    pair = list(pair)
                    item = self.gisScene.GraphicsItemLogicalMapping['cgi'].get(pair[0])
                    if item != None:
                        self.__drawLineToTransmitter(item.sceneBoundingRect().center(),pair[1],COARFCN_PEN)
                    else:
                        self.logger.warn('CGI {} detected for conflict with {},but not on GIS map.'.format(pair[0],pair[1]))

    def onContextMenuForClusterViewer(self,EventQPoint):
        #self.logger.debug('Right Menu is required!')
        self.clusterViewerPopupMenu.popup(self.clusterViewerTable.mapToGlobal(EventQPoint))

    def showSelectedClusters(self):
        self.gisScene.clearCellTraceLine()
        if self.clusterViewerTable.selectedRanges():
            for idx in self.clusterViewerTable.selectedIndexes():
                #self.logger.debug('cluster:{}'.format(self.clusterViewerTable.item(idx.row(),0).value()))
                clusterid = self.clusterViewerTable.item(idx.row(),0).value()
                if clusterid:
                    self.showCellsByClusterID(clusterid,False)

    def genMaximalConnecedtClusterReport(self):
        questDialog = MaximalConnectedClusterDialog(self)
        if questDialog.exec_():
            self.workingLabor.shoot(False,self.controller.calcSaveMaximalConnectedClusterReport,unicode(questDialog.MatchedMREdit.text()),unicode(questDialog.ClusterEdit.text()))

    def calcMaximalConnectedCluster(self):
        self.workingLabor.shoot(False,self.controller.calcMaximalConnectedCluster)

    def runConflictingTRXResolver(self,filepath,ARFCNs,TargetCells):
        self.logger.info('Start RF RePlan task,running....')
        self.logger.debug('{},{},{}'.format(filepath,ARFCNs,TargetCells))
        self.workingThread = threading.Thread(target = self.controller.startRFRePlanProcedure,name = 'RF Replan',args=( filepath,ARFCNs,TargetCells))
        self.workingThread.start()

    def onBackgroundTaskComplete(self,taskName):
        self.logger.debug('Task {} complete.'.format(taskName))
        if taskName in ('DATAPARSER','OPENPROJECT'):
            self.workingLabor.shoot(True,self.controller.getCgiList)
            if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                result = self.workingLabor.runnerResult
                comp = QtGui.QCompleter(result)
                #comp.setCompletionPrefix('460-00-')
                self.cgiToFindEdit.setCompleter(comp)
        if taskName == 'OPENPROJECT':
            if not self.workingLabor.runnerError:
                self.updateWindowTitle(self.controller.projectName)
            else:
                self.logger.error('Project open failed!')

        if taskName == 'GLOBAL_COARFCN_CHECK':
            if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                self.logger.info('Global CoARFCN success with result.')
                result = self.workingLabor.runnerResult
                self.onGlobalCoArfcnCheckCompleted(result)
            else:
                self.logger.error('Global CoARFCN check failed.!')


    def locateCellFromCellCoverageTable(self,row,column):
        #self.logger.debug('clicked at {},{}'.format(row,column))
        if unicode(self.CellCoverageInfomationTable.item(row,0).text()) in self.gisScene.GraphicsItemLogicalMapping.get('cgi',{}):
            item = self.gisScene.GraphicsItemLogicalMapping.get('cgi').get(unicode(self.CellCoverageInfomationTable.item(row,0).text()))
            self.gisViewer.centerOn(item)
            self.gisScene.toggleGraphicItemHighlightStatus([item,])

    def onFindCellHandler(self):
        if unicode(self.cgiToFindEdit.text()):
            self.logger.debug('Try find {}'.format(unicode(self.cgiToFindEdit.text())))
            targetItem = self.gisScene.GraphicsItemLogicalMapping.get('cgi',{}).get(unicode(self.cgiToFindEdit.text()))
            if targetItem:
                self.gisScene.toggleGraphicItemHighlightStatus([targetItem,])
                self.gisViewer.centerOn(targetItem)
                #self.logger.debug('I am at {},{}'.format(targetItem.x(),targetItem.y()))

    def createAction(self, text, slot=None, shortcut=None, icon=None,tip=None, checkable=False, signal="triggered()"):
        action = QtGui.QAction(text, self)
        if icon is not None:
            action.setIcon(QtGui.QIcon(":/{0}.png".format(icon)))
        if shortcut is not None:
            action.setShortcut(shortcut)
        if tip is not None:
            action.setToolTip(tip)
            action.setStatusTip(tip)
        if slot is not None:
            self.connect(action, QtCore.SIGNAL(signal), slot)
        if checkable:
            action.setCheckable(True)
        return action

    def addActions(self, target, actions):
        for action in actions:
            if action is None:
                target.addSeparator()
            else:
                target.addAction(action)

    def updateWindowTitle(self,projectName):
        self.setWindowTitle('NSA [{}]'.format(projectName))

    def enterPolygonSelectionMode(self,callback = None):
        if self.ObjectTraceAction.isChecked():
            self.ObjectTraceAction.trigger()
        self.gisViewer.enablePolygonSelectionMode()
        if callback:
            self.gisViewer.polygonSelectCompleteSignal.connect(callback)

    def resetMapStatus(self):
        self.gisScene.clearCellTraceLine()
        self.gisScene.toggleGraphicItemHighlightStatus([])

    def toggleBackgroundMap(self):
        if self.enableMapBackgroundAction.isChecked():
            self.gisViewer.enableBackgroundMap()
        else:
            self.gisViewer.disableBackgroundMap()

    def toggleObjectTraceMode(self):
        if self.ObjectTraceAction.isChecked():
            self.gisViewer.cursorMode = 'traceObject'
            self.gisViewer.setMouseTracking(True)
            self.logger.info('Enter GIS Object tracing mode.')
            self.gisViewer.setDragMode(QtGui.QGraphicsView.NoDrag)
            #self.preCursor = self.gisViewer.cursor()
            #self.gisViewer.setCursor(QtGui.QCursor(QtCore.Qt.BlankCursor))
        else:
            self.gisViewer.cursorMode = 'None'
            self.gisViewer.setMouseTracking(False)
            self.logger.info('Exit GIS Object tracing mode.')
            self.gisViewer.setDragMode(QtGui.QGraphicsView.ScrollHandDrag)
            try:
                self.gisViewer.preDetectedItem.setBrush(self.gisViewer.preDetectedItemBrush)
            except:
                pass
            try:
                self.gisScene.removeItem(self.gisViewer.horizontalTraceLine)
                self.gisScene.removeItem(self.gisViewer.verticalTraceLine)
            except:
                pass

    def openProjectHandler(self):
        fileName = unicode(QtGui.QFileDialog.getOpenFileName(filter = 'NSA project (*.nsa *.db3)'))
        if fileName:
            self.workingLabor.shoot(False,self.controller.openProject,fileName)
#            if not self.workingLabor.runnerError:
#                self.updateWindowTitle(self.controller.projectName)
#            else:
#                self.logger.error('Project open failed!')

    def saveAsProjectHandler(self):
        fileName = unicode(QtGui.QFileDialog.getSaveFileName(filter = 'NSA SQL Dump (*.nsa)'))
        if fileName:
            self.logger.info('Project saving as {}...'.format(fileName))
            #if self.controller.saveAsProject(fileName):
            self.workingLabor.shoot(True,self.controller.saveAsProject,fileName)
            if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                self.logger.info('Project saved as {}.'.format(fileName))
            else:
                self.logger.error('Project save failed.')

    def newProjectGuideHandler(self):
        diag = NewProjectDialog(self)
        if diag.exec_():
            self.utmZone = diag.ProjectUTMZoneSpinBox.value()
            self.workingLabor.shoot(True,self.controller.createNewProject,unicode(diag.ProjectPathEdit.text()),unicode(diag.ProjectNameEdit.text()),diag.ProjectUTMZoneSpinBox.value())
            #self.controller.createNewProject(unicode(diag.ProjectPathEdit.text()),unicode(diag.ProjectNameEdit.text()),diag.ProjectUTMZoneSpinBox.value())

    def dataLoadHandler(self,dataSet):
        self.logger.debug('MainWindos.dataLoadHandler triggered')
        self.workingLabor.shoot(False,self.controller.dataParserThread,dataSet)

    def dataManagerDispatcher(self):
        diag = DataManagerDialog(self)
        try:
            #self.connect(diag,QtCore.SIGNAL('loadRequest'),self.controller.DataLoadHandler)
            self.connect(diag,QtCore.SIGNAL('loadRequest'),self.dataLoadHandler)
        except:
            self.logger.exception('SIGNAL loadRequest(dict) connect failed!')
            raise
        diag.exec_()

    def clearSiteFromView(self):
        #TODO need fix not successfully clean uninstall
        self.logger.debug('clear site from view.')
        for item in self.gisScene.items():
            if hasattr(item,'logicalType') and item.logicalType in ('cgi','geo'):
                self.gisScene.removeItem(item)

        self.gisScene.GraphicsItemLogicalMapping['geo'] = {}
        self.gisScene.GraphicsItemLogicalMapping['cgi'] = {}
    def clearScanPlotFromView(self):
        for item in self.gisScene.GraphicsItemLogicalMapping.get('scanSample',{}).values():
            self.gisScene.removeItem(item)
        self.gisScene.GraphicsItemLogicalMapping['scanSample'] = {}

    def CalcDrawTemsScanLogBySSI(self):
        #Draw TEMS scan log by street structual index
        infDb = self.ScanCoverageCriterionDockWidget.widget().RelativeSpinBox.value()
        availTCHs = self.ScanCoverageCriterionDockWidget.widget().AvailableTchSpinBox.value()
        self.logger.info('Start draw Scan SSI samples,inferior dB {},max tch {}.'.format(infDb,availTCHs))
        self.workingLabor.shoot(True,self.controller.calcStreetStructualFeature,infDb)
        if not self.workingLabor.runnerError:
            result = self.workingLabor.runnerResult if self.workingLabor.runnerResult else {}
            #dataSet[(sample['longitude'],sample['latitude'])] = ((sample['x'],sample['y']),trxCount,cellCount)
            self.clearScanPlotFromView()
            for coordKey in result:
                if self.gisScene.GraphicsItemLogicalMapping.get('scanSample',{}).get(coordKey):
                    self.logger.error('Found duplicated scan sample on {} {}'.format(coordKey[0],coordKey[1]))
                else:
                    #coordX = result[coordKey][0][0]
                    #coordY = -1 * result[coordKey][0][1]
                    coordX = result[coordKey][3][0]
                    coordY = -1 * result[coordKey][3][1]
                    streetIndex = result[coordKey][1]/float(availTCHs)
                    if streetIndex < 0.7:
                        color = QtGui.QColor(144,238,144,200)
                    elif 0.7 <= streetIndex < 1:
                        color = QtGui.QColor(252,236,10,200)
                    else:
                        color = QtGui.QColor(255,0,0,200)

                    factor = max(streetIndex,1)
                    item = self.gisScene.addEllipse(coordX - 5*factor,coordY - 5*factor,10*factor,10*factor,QtGui.QPen(color),QtGui.QBrush(color,QtCore.Qt.SolidPattern))
                    item.setZValue(LAYER_SCAN)
                    if item:
                        item.logicalType = 'scanSample'
                        item.logicalId = coordKey
                        item.streetIndex = streetIndex
                        item.streetCells = result[coordKey][2]
                        item.streetTCHs = result[coordKey][1]
                        try:
                            self.gisScene.GraphicsItemLogicalMapping['scanSample'][item.logicalId] = item
                        except KeyError:
                            self.gisScene.GraphicsItemLogicalMapping['scanSample'] = {}
                            self.gisScene.GraphicsItemLogicalMapping['scanSample'][item.logicalId] = item
        self.logger.info('Draw fmt scan SSI sample done.')

    def drawFMTScanLog(self):
        #TODO need to check if it is steady
        #        sample = {}
        #        sample['x'] = geoRow[2]
        #        sample['y'] = geoRow[3]
        #        sample['longitude'] = geoRow[0]
        #        sample['latitude'] = geoRow[1]
        #        sample['samples'] = []
        #        sample['maxRxlev'] = 0
        #        sampleCursor.execute(sampleSelectSql,(geoRow[0],geoRow[1]))
        #        for clip in sampleCursor:
        #            sample['samples'].append([value for value in clip])
        #            sample['maxRxlev'] = max(sample['maxRxlev'],clip[3])
        self.logger.info('Start draw fmt scan samples.')
        #self.workingLabor.shoot(True,self.controller.getFMTScanedSamplesIterator)
        self.workingLabor.shoot(True,self.controller.getFMTScanedSamples)
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            self.clearScanPlotFromView()
            result = self.workingLabor.runnerResult
            for sample in result:
                if self.gisScene.GraphicsItemLogicalMapping.get('scanSample',{}).get((sample['longitude'],sample['latitude'])):
                    self.logger.error('Found duplicated scan sample on {} {}'.format(sample['longitude'],sample['latitude']))
                else:
                    #coordX = sample['x']
                    #coordY = -1 * sample['y']
                    coordX = sample['projx']
                    coordY = -1 * sample['projy']
                    item = self.gisScene.addEllipse(coordX - 5,coordY - 5,10,10,QtGui.QPen(QtGui.QColor(144,238,144,200)),QtGui.QBrush(QtGui.QColor(144,238,144,200),QtCore.Qt.SolidPattern))
                    item.setZValue(LAYER_SCAN)
                    if item:
                        item.logicalType = 'scanSample'
                        item.logicalId = (sample['longitude'],sample['latitude'])
                        try:
                            self.gisScene.GraphicsItemLogicalMapping['scanSample'][item.logicalId] = item
                        except KeyError:
                            self.gisScene.GraphicsItemLogicalMapping['scanSample'] = {}
                            self.gisScene.GraphicsItemLogicalMapping['scanSample'][item.logicalId] = item
        self.logger.info('Draw fmt scan sample done.')

    def drawSiteView(self):
        self.clearSiteFromView()
        self.workingLabor.shoot(True,self.controller.getGSMCellNetworkInfo)
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            GcellNetworkInfo = self.workingLabor.runnerResult
            self.controller.calcDelauNeis()
            #sql = 'SELECT cgi,bcch,x,y,dir,coverage_type,longitude,latitude,cellname,bsic,extcell,tile,projx,projy FROM cellNetworkInfo'
            try:
                for row in GcellNetworkInfo:
                    if row[12] and row[13]:
                        coordX = row[12]
                        coordY = -1 * row[13]
                        geo = (row[6],row[7])
                        if self.gisScene.GraphicsItemLogicalMapping.get('geo',{}).get(geo):
                            pass
                        else:
                            gitem = self.gisScene.addEllipse(coordX-6/2,coordY-6/2,6,6)
                            gitem.logicalType = 'geo'
                            gitem.logicalId = geo
                            gitem.setZValue(1)
                            try:
                                self.gisScene.GraphicsItemLogicalMapping['geo'][gitem.logicalId] = gitem
                            except KeyError:
                                self.gisScene.GraphicsItemLogicalMapping['geo'] = {}
                                self.gisScene.GraphicsItemLogicalMapping['geo'][gitem.logicalId] = gitem
                        item = None
                        if self.gisScene.GraphicsItemLogicalMapping.get('cgi',{}).get(row[0]):
                            self.logger.warn('CGI {} already add to the GIS Viewer,duplicate avoid.')
                            continue
                        if row[5] in ('macro',):
                            factor = self.controller.getSiteDistbyCGI(row[0])
                            if row[1]  and row[4] is not None and row[1] < 512:
                                #FOR GSM900
                                item = self.gisScene.addPath(self.__getMacroTransmitterPath(coordX,coordY,length=50*factor),brush=QtGui.QBrush(COLOR900))
                                item.setTransformOriginPoint(coordX,coordY)
                                item.setRotation(row[4])
                            elif row[1]  and row[4] is not None:
                                #FOR GSM1800
                                #item = self.__CreateMacroTransmitterGraphicItem(coordX,coordY,row[4],90,30)
                                item = self.gisScene.addPath(self.__getMacroTransmitterPath(coordX,coordY,90,length=30*factor),brush=QtGui.QBrush(COLOR1800))
                                item.setTransformOriginPoint(coordX,coordY)
                                item.setRotation(row[4])
                            elif row[4] is not None:
                                #item = self.__CreateMacroTransmitterGraphicItem(coordX,coordY,row[4],length = 20)
                                self.logger.warn('Cell {} missing BCCH , Antena Direction {}'.format(row[0],row[4]))
                                item = self.gisScene.addPath(self.__getMacroTransmitterPath(coordX,coordY,length = 20*factor),brush=QtGui.QBrush(COLORUNKNOWN))
                                item.setTransformOriginPoint(coordX,coordY)
                                item.setRotation(row[4])
                            else:
                                self.logger.warn('Escaping drawing cell {},invalid BCCH {} or Antena Direction {}'.format(row[0],row[1],row[4]))
                        elif row[5] in ('indoor','underlayer'):
                            if row[1]:
                                color = COLOR900 if row[1] < 512 else COLOR1800
                                item = self.gisScene.addEllipse(coordX-20/2,coordY-20/2,20,20,brush=QtGui.QBrush(color))
                            else:
                                item = self.gisScene.addEllipse(coordX-10/2,coordY-10/2,10,10,brush = QtGui.QBrush(COLORUNKNOWN))
                                self.logger.info('CGI {} in Cell GeoSets is not in network Sets.'.format(row[0]))
                        else:
                            self.logger.error(u'Unsupported cell coverage type:{}'.format(row[5]))
                        if item:
                            item.logicalType = 'cgi'
                            item.logicalId = row[0]
                            item.setZValue(LAYER_CELL)
                            try:
                                self.gisScene.GraphicsItemLogicalMapping['cgi'][item.logicalId] = item
                            except KeyError:
                                self.gisScene.GraphicsItemLogicalMapping['cgi'] = {}
                                self.gisScene.GraphicsItemLogicalMapping['cgi'][item.logicalId] = item
                    else:
                        #TODO need more action for none extcells
                        pass
            except:
                self.logger.exception('Found unexpected error when drawing cell transmitters!')
        else:
            self.logger.error('Failed to get getGSMCellNetworkInfo,drawing failed!')

    def generateCellCoverageReportByScanData(self):
        self.logger.info('Start to generate coverage report by scan data.')
        if not hasattr(self,'wizard'):
            self.wizard = CellCoveragePerformanceReportDialog(self)
        if not self.wizard.exec_():
            return False
        fileToSave = unicode(QtGui.QFileDialog.getSaveFileName(self,filter = 'CSV File (*.csv)'))
        if not fileToSave:
            return
        self.workingLabor.shoot(False,self.controller.genCellCoverageReportByScanData,fileToSave,
            self.wizard.iRxlevCellSpinBox.value(),
            self.wizard.dRxlevCellSpinBox.value(),
            self.wizard.aRxlevCellSpinBox.value(),
            self.wizard.iRxlevSSISpinBox.value(),
            self.wizard.availTchCountSpinBox.value(),
            self.wizard.SSIFilterSpinBox.value()
        )

    def generateGridMap(self):
        filePath = QtGui.QFileDialog.getSaveFileName(filter = ('Mapinfo Interchange Format (*.mif)'))
        if filePath:
            self.logger.info('Raster KPI data saving at {}'.format(filePath))

    def calcCellCoverageStatisticsByScanData(self):
        #Show global cell coverage info from scan samples
        self.logger.info('Start to calc coverage statistics by scan data.')
        if not hasattr(self,'wizard'):
            self.wizard = CellCoveragePerformanceReportDialog(self)
        if not self.wizard.exec_():
            return False
        self.workingLabor.shoot(True,self.controller.calcCellCoverageReportByScanData,
            self.wizard.iRxlevCellSpinBox.value(),
            self.wizard.dRxlevCellSpinBox.value(),
            self.wizard.aRxlevCellSpinBox.value(),
            self.wizard.iRxlevSSISpinBox.value(),
            self.wizard.availTchCountSpinBox.value(),
            self.wizard.SSIFilterSpinBox.value()
        )
        if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
            result = self.workingLabor.runnerResult
            self.CellCoverageInfomationTable.setSortingEnabled(False)
            for removeIdx in range(self.CellCoverageInfomationTable.rowCount()):
                self.CellCoverageInfomationTable.removeRow(removeIdx)
            self.CellCoverageInfomationTable.setRowCount(len(result))
            rowCount = 0
            for cgi in result:
                columnCount = 0
                self.CellCoverageInfomationTable.setItem(rowCount,columnCount,QtGui.QTableWidgetItem('{}'.format(cgi)))
                columnCount = 1
                for key in self.controller.cellCoverageResultDictIndex:
                    self.CellCoverageInfomationTable.setItem(rowCount,columnCount,NumberTableWidgetItem(result[cgi][key]))
                    columnCount += 1

                try:
                    #['Dominate/(Dominate + Interfer)'
                    self.CellCoverageInfomationTable.setItem(rowCount,columnCount,NumberTableWidgetItem(1.0*result[cgi]['cds'] / (result[cgi]['cds']+result[cgi]['cis'])))
                except:
                    pass
                finally:
                    columnCount += 1

                try:
                    #,'OverlappedDominate/(Dominate)'
                    self.CellCoverageInfomationTable.setItem(rowCount,columnCount,NumberTableWidgetItem(1.0*result[cgi]['cods'] / result[cgi]['cds']))
                except:
                    pass
                finally:
                    columnCount += 1

                try:
                    #,'Overlapped/(Dominate + Interfer)']
                    self.CellCoverageInfomationTable.setItem(rowCount,columnCount,NumberTableWidgetItem(1.0*(result[cgi]['cois'] + result[cgi]['cods'])/ (result[cgi]['cds']+result[cgi]['cis'])))
                except:
                    pass
                finally:
                    columnCount += 1
                rowCount += 1

            self.CellCoverageInfomationTable.setSortingEnabled(True)
            self.CellCoverageInfomationDockWidget.raise_()
            self.CellCoverageInfomationDockWidget.show()
        else:
            self.logger.error('Can not get cell coverage statistics data.')

    def __getMacroTransmitterPath(self,x,y,AngleRange=30,length = 50):
        transmitter_path = QtGui.QPainterPath()
        transmitter_path.moveTo(x,y)
        #transmitter_path.arcTo(QtCore.QRectF(x-length/2,y-length*2,length,length*2),90-AngleRange/2,AngleRange)
        transmitter_path.arcTo(QtCore.QRectF(x-length,y-length*2,length*2,length*2),90-AngleRange/2,AngleRange)
        transmitter_path.closeSubpath()
        return transmitter_path

    def onGraphicsItemDetect(self,logicalItem,posFromScene):
        '''
        Response to Logical Item Focus event
        '''
        #Draw spot to cell line
        if self.ScanSpotCoverageAnalysisAction.isChecked():
            if not self.ScanInfoTable.isVisible():
                self.ScanInfoTable.parent().show()
            if hasattr(logicalItem,'logicalType') and getattr(logicalItem,'logicalType') == 'scanSample':
                for litem in self.gisScene.GraphicsItemCellTraceLines:
                    #TODO Need fix,item in different scene
                    self.gisScene.removeItem(litem)
                self.gisScene.GraphicsItemCellTraceLines = []

                lid = getattr(logicalItem,'logicalId')
                self.workingLabor.shoot(True,self.controller.getFMTScanSampleByCoords,lid[0],lid[1])
                if not self.workingLabor.runnerError:
                    samples = self.workingLabor.runnerResult
                if samples and samples['samples']:
                    #self.logger.info('MaxRxlev:{}'.format(samples['maxRxlev']))
                    relativeDB = self.ScanCoverageCriterionDockWidget.widget().RelativeSpinBox.value()
                    absoluteDB = self.ScanCoverageCriterionDockWidget.widget().AbsoluteSpinBox.value()
                    #TODO FIXME setRowCount won't work
                    self.ScanInfoTable.setSortingEnabled(False)
                    for removeIdx in range(self.ScanInfoTable.rowCount()):
                        #self.ScanInfoTable.removeRow(removeIdx)
                        self.ScanInfoTable.removeRow(0)
                    self.ScanInfoTable.setRowCount(len(samples['samples']))
                    rowCount = 0
                    for lsample in samples['samples']:
                        #sampleSelectSql = 'SELECT cgi,bcch,bsic,arxlev,samples,distance,ref_distance FROM raw_fmt_aggregate WHERE longitude = ? AND latitude = ? ORDER BY arxlev DESC'
                        columnCount = 0
                        for value in lsample:
                            #self.ScanInfoTable.setItem(rowCount,columnCount,QtGui.QTableWidgetItem(unicode(value)))
                            self.ScanInfoTable.setItem(rowCount,columnCount,NumberTableWidgetItem(value))
                            columnCount += 1
                        rowCount += 1
                        if lsample[0]:
                            if lsample[3] >= max(samples['maxRxlev'] - relativeDB,absoluteDB):
                            #if samples['maxRxlev'] - logicalItem[3] <= 12:
                                if lsample[2] == 'NULL':
                                    if lsample[3] == samples['maxRxlev']:
                                        DASH_LINE_PEN.setColor(STRONGEST_COLOR)
                                    else:
                                        DASH_LINE_PEN.setColor(NORMAL_DASH_COLOR)
                                    self.__drawLineToTransmitter(posFromScene,lsample[0],DASH_LINE_PEN)
                                else:
                                    if lsample[3] == samples['maxRxlev']:
                                        SOLID_LINE_PEN.setColor(STRONGEST_COLOR)
                                    else:
                                        SOLID_LINE_PEN.setColor(NORMAL_SOLID_COLOR)
                                    self.__drawLineToTransmitter(posFromScene,lsample[0],SOLID_LINE_PEN)
                    self.ScanInfoTable.setSortingEnabled(True)
                    coordsInReal = self.realProj(*self.mapProj(posFromScene.x(),-1*posFromScene.y(),inverse = True))
                    #self.workingLabor.shoot(True,self.controller.getDelnySurrondingCells,(posFromScene.x(),-1*posFromScene.y()),[lsample[0] for lsample in samples['samples'] if lsample[0]])
                    self.workingLabor.shoot(True,self.controller.getDelnySurrondingCells,coordsInReal,[lsample[0] for lsample in samples['samples'] if lsample[0]])
                    if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                        result = self.workingLabor.runnerResult
                        try:
                            self.gisScene.toggleGraphicItemHighlightStatus([self.gisScene.GraphicsItemLogicalMapping['cgi'][cgi] for cgi in result if cgi in self.gisScene.GraphicsItemLogicalMapping['cgi']])
                        except KeyError:
                            pass

                    if hasattr(logicalItem,'streetIndex'):
                        self.StreetCoverageInfoDockWidget.widget().ssiCountLabel.setText('{}'.format(getattr(logicalItem,'streetIndex')))
                    else:
                        self.StreetCoverageInfoDockWidget.widget().ssiCountLabel.setText('N/A')
                    if hasattr(logicalItem,'streetCells'):
                        self.StreetCoverageInfoDockWidget.widget().cellCountLabel.setText('{}'.format(getattr(logicalItem,'streetCells')))
                    else:
                        self.StreetCoverageInfoDockWidget.widget().cellCountLabel.setText('N/A')
                    if hasattr(logicalItem,'streetTCHs'):
                        self.StreetCoverageInfoDockWidget.widget().tchCountLabel.setText('{}'.format(getattr(logicalItem,'streetTCHs')))
                    else:
                        self.StreetCoverageInfoDockWidget.widget().tchCountLabel.setText('N/A')

        #draw cell to spot line
        if self.ScanCellCoverageAnalysisAction.isChecked():
            if hasattr(logicalItem,'logicalType') and getattr(logicalItem,'logicalType') == 'cgi':
                for litem in self.gisScene.GraphicsItemCellTraceLines:
                    #TODO Need fix,item in different scene
                    self.gisScene.removeItem(litem)
                self.gisScene.GraphicsItemCellTraceLines = []
                lid = getattr(logicalItem,'logicalId')

                #Get cell coverage samples
                self.workingLabor.shoot(True,self.controller.getFMTScanSampleByMatchedCGI,lid)
                if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                    samples = self.workingLabor.runnerResult
                    for lsample in samples:
                        if lsample[4] >= self.ScanCoverageCriterionDockWidget.widget().AbsoluteSpinBox.value() and  lsample[-1] - lsample[4] <= self.ScanCoverageCriterionDockWidget.widget().RelativeSpinBox.value():
                            if lsample[5] != "NULL":
                                if lsample[4] == lsample[-1]:
                                    SOLID_LINE_PEN.setColor(STRONGEST_COLOR)
                                else:
                                    SOLID_LINE_PEN.setColor(NORMAL_SOLID_COLOR)
                                self.__drawLineToTransmitter(QtCore.QPointF(lsample[8],-1*lsample[9]),lid,SOLID_LINE_PEN)
                            else:
                                if lsample[4] == lsample[-1]:
                                    DASH_LINE_PEN.setColor(STRONGEST_COLOR)
                                else:
                                    DASH_LINE_PEN.setColor(NORMAL_DASH_COLOR)
                                self.__drawLineToTransmitter(QtCore.QPointF(lsample[8],-1*lsample[9]),lid,DASH_LINE_PEN)

        #Get cell detailed info
        if hasattr(logicalItem,'logicalType') and getattr(logicalItem,'logicalType') == 'cgi':
            lid = getattr(logicalItem,'logicalId')
            self.workingLabor.shoot(True,self.controller.getGsmCellDetailedInfo,lid)
            if not self.workingLabor.runnerError and self.workingLabor.runnerResult:
                result = self.workingLabor.runnerResult
                self.updateCellInfoTable(result)
                self.CellInfoDockWidget.show()
        return  True

    def __drawLineToTransmitter(self,startPointF,TargetCGI,pen = QtGui.QPen()):
        if self.gisScene.GraphicsItemLogicalMapping.get('cgi'):
            item = self.gisScene.GraphicsItemLogicalMapping['cgi'].get(TargetCGI)
            if item:
                TransmitterCenterPoint = item.sceneBoundingRect().center()
                line = self.gisScene.addLine(startPointF.x(),startPointF.y(),TransmitterCenterPoint.x(),TransmitterCenterPoint.y(),pen)
                line.setZValue(LAYER_DYNAMIC)
                self.gisScene.GraphicsItemCellTraceLines.append(line)
                return True
        return False

    def resetProgressBar(self):
        self.runningProgressBar.setValue(0)

    def tickProgressBar(self):
        self.runningProgressBar.setValue(self.runningProgressBar.value()+1)

    def setProgressBar(self,value):
        self.runningProgressBar.setValue(value)

    def onTemsScanCellCoverageAction(self):
        if self.ScanSpotCoverageAnalysisAction.isChecked() or self.ScanCellCoverageAnalysisAction.isChecked():
            if not self.ObjectTraceAction.isChecked():
                self.ObjectTraceAction.activate(QtGui.QAction.Trigger)
                self.gisScene.toggleGraphicItemHighlightStatus()
        elif self.ObjectTraceAction.isChecked():
            self.ObjectTraceAction.activate(QtGui.QAction.Trigger)
            self.gisScene.toggleGraphicItemHighlightStatus()
        if not self.ScanCellCoverageAnalysisAction.isChecked():
            for litem in self.gisScene.GraphicsItemCellTraceLines:
                #TODO Need fix,item in different scene
                self.gisScene.removeItem(litem)
            self.gisScene.GraphicsItemCellTraceLines = []

    def onTemsScanSpotCoverageAction(self):
        if self.ScanSpotCoverageAnalysisAction.isChecked() or self.ScanCellCoverageAnalysisAction.isChecked():
            if not self.ObjectTraceAction.isChecked():
                self.ObjectTraceAction.activate(QtGui.QAction.Trigger)
        elif self.ObjectTraceAction.isChecked():
            self.ObjectTraceAction.activate(QtGui.QAction.Trigger)
        if not self.ScanSpotCoverageAnalysisAction.isChecked():
            for litem in self.gisScene.GraphicsItemCellTraceLines:
                #TODO Need fix,item in different scene
                self.gisScene.removeItem(litem)
            self.gisScene.GraphicsItemCellTraceLines = []
            self.gisScene.toggleGraphicItemHighlightStatus()
