# -*- coding: cp936 -*
__author__ = 'highland'


import csv
#import sqlite3
import pysqlite2.dbapi2 as sqlite3
import re
import os
import fnmatch
from math import sqrt
import weakref
import random
import logging
import shelve
import threading
import pyproj
import shapely.geometry
import delaunay
import bz2

#import NSA
import _globals
import dumpSqlite3

class ImprovedShelf(shelve.DbfilenameShelf):
    #Add __iter__ to Shelf Class
    def __init__(self, filename, flag='c', protocol=None, writeback=False):
        shelve.DbfilenameShelf.__init__(self, filename,flag , protocol, writeback)
        self.logger = logging.getLogger('global')

    def __iter__(self):
        self.logger.info('Iter entered...')
        for k in self.keys():
            self.logger.info('Yield key {}'.format(k))
            yield k

class TemsScanParserClass(object):

    def __init__(self,parent = None):
        self.parent = parent
        self.logger = logging.getLogger('global')
        self.fmtSamples = 0
        self.Proj = None
        self.maxBsicMatchRange = 5000 #unit meters
        self.maxBcchMatchRange = 500 #unit meters
        self.isMatched = False

    def fmtParser(self,fmtFiles):
        self.logger.info('{} started...'.format('fmtParser'))
        bsicMatcher = re.compile('All-Scanned BSIC On ARFCN\[([0-9]*)\]')
        rxlevMatcher = re.compile('All-Scanned RxLev On ARFCN\[([0-9]*)\]')
        #self.parent.resetProgressBarEvent.set()
        if not fmtFiles:
            return  False
        try:
            dbConn = self.parent._acquireDBConn()
            if not dbConn:
                self.logger.error('Database connection not created!')
                self.parent._releaseDBConn()
                return False
            dbConn.execute('DROP TABLE IF EXISTS raw_fmt')
            dbConn.execute('CREATE TABLE raw_fmt (longitude REAL,latitude REAL,bcch INTEGER,bsic TEXT,rxlev INTEGER)')
            dbConn.commit()

            for fmt in fmtFiles:
                try:
                    with open(fmt,'rb') as fp:
                        recordCount = 0
                        self.logger.info(u'processing {}...'.format(fmt))
                        reader = csv.reader(fp,dialect = csv.excel_tab)
                        header = reader.next()
                        idx_time = header.index('Time')
                        idx_long = header.index('All-Longitude')
                        idx_lat = header.index('All-Latitude')
                        idx_bsic_onArfcn = {}
                        idx_rxlev_onArfcn = {}
                        for col in header:
                            if bsicMatcher.findall(col):
                                idx_bsic_onArfcn[bsicMatcher.findall(col)[0]] = header.index(col)
                            if rxlevMatcher.findall(col):
                                idx_rxlev_onArfcn[rxlevMatcher.findall(col)[0]] = header.index(col)

                        sampleHash = None
                        for row in reader:
                            recordCount+=1
                            mark = [row[idx_long],row[idx_lat]]
                            try:
                                for arfcn in idx_bsic_onArfcn:
                                    mark.append('{}:{}'.format(row[idx_bsic_onArfcn[arfcn]],row[idx_rxlev_onArfcn[arfcn]]))
                            except IndexError:
                                self.logger.warn(u'Unmatched bsic & rxlev pair or missing column,file {} escaped.'.format(fmt))
                                continue

                            if hash(tuple(mark)) != sampleHash :
                                #new sample record found
                                sampleHash = hash(tuple(mark))

                            try:
                                dataSet = []
                                for arfcn in idx_rxlev_onArfcn:
                                    rxlev = row[idx_rxlev_onArfcn[arfcn]].strip()
                                    bsic = row[idx_bsic_onArfcn[arfcn]].strip()
                                    if rxlev and '111' != rxlev:
                                        dataSet.append((row[idx_long],row[idx_lat],arfcn,'NULL' if not bsic else bsic,rxlev))
                                dbConn.executemany('INSERT INTO raw_fmt VALUES (?,?,?,?,?)',dataSet)
                                if recordCount % 5000 == 0:
                                    dbConn.commit()
                            except:
                                self.logger.exception('Error occured when insert FMT raw log into database,record {}'.format(recordCount))
                                self.logger.error(u'Error data:{}'.format(row))
                                raise
                        dbConn.commit()
                        self.logger.info('{} unique samples imported.'.format(recordCount))
                except:
                    self.logger.exception(u'Failed to process {} due to unknown error,some data imported!'.format(fmt))
                    continue
            dbConn.execute('DELETE FROM projectinfo WHERE attribute = "dataload" AND value = "rawScan"')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","rawScan",datetime("now"))')
            dbConn.commit()
            self.parent._releaseDBConn()
            return True
        finally:
            try:
                self.parent._releaseDBConn()
            except:
                pass
            self.logger.info('{} done.'.format('fmtParser'))


    def _fmt_average(self,dropRawData = True):
        self.logger.info('FMT post import process started...')
        #self.Proj = pyproj.Proj(proj='utm',zone = self.parent.utmZone,ellps='WGS84')
        self.realProj = _globals.getCalcProj()
        self.mapProj = _globals.getViewerProj()

        dbConn = self.parent._acquireDBConn()
        if not dbConn:
            self.logger.error('Database connection not created!')
            self.parent._releaseDBConn()
            return False
        try:
            dbConn.execute('DROP TABLE IF EXISTS raw_fmt_aggregate')
            dbConn.execute('CREATE TABLE raw_fmt_aggregate (rowid INTEGER PRIMARY KEY AUTOINCREMENT,longitude REAL,latitude REAL,x REAL,y REAL,projx REAL,projy REAL,bcch INTEGER,bsic TEXT,arxlev REAL,samples INTEGER,cgi TEXT,distance REAL,ref_distance REAL)')
            dbConn.commit()
            cursor = dbConn.cursor()
            cursor.execute('SELECT longitude,latitude,bcch,bsic,AVG(rxlev) as arxlev,COUNT(*) as samples FROM raw_fmt GROUP BY longitude,latitude,bcch,bsic')
            self.logger.info('Aggregate sample done.Saving result...')
            fetchSize = 10000
            resultSets = cursor.fetchmany(size = fetchSize)
            insertRecordsCount = 0
            while len(resultSets) > 0:
                dataSet = []
                for row in resultSets:
                    try:
                        #dataSet.append(row[0:2]+self.Proj(row[0],row[1])+row[2:])
                        dataSet.append(row +self.realProj(row[0],row[1])+self.mapProj(row[0],row[1]))
                    except TypeError:
                        self.logger.exception('Failed to convert {} in UTM Zone {} or Merc'.format(row[0:2],self.parent.utmZone))
                dbConn.executemany('INSERT INTO raw_fmt_aggregate (longitude ,latitude ,bcch ,bsic ,arxlev ,samples,x ,y,projx,projy) VALUES (?,?,?,?,?,?,?,?,?,?)',dataSet)
                dbConn.commit()
                insertRecordsCount += len(resultSets)
                self.logger.info('{} records saved,running...'.format(insertRecordsCount))
                resultSets = cursor.fetchmany(size = fetchSize)
            self.logger.info('All done without error,totol records {}.'.format(insertRecordsCount))
            self.fmtSamples = insertRecordsCount
            dbConn.execute('CREATE INDEX IF NOT EXISTS fmt_x ON raw_fmt_aggregate (x ASC)')
            dbConn.execute('CREATE INDEX IF NOT EXISTS fmt_y ON raw_fmt_aggregate (y ASC)')
            dbConn.execute('CREATE INDEX IF NOT EXISTS fmt_bsic_index ON raw_fmt_aggregate (longitude ASC,latitude ASC,bcch,bsic)')
            dbConn.commit()
            dbConn.execute('DELETE FROM projectinfo WHERE attribute = "dataload" AND value = "avgScan"')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","avgScan",datetime("now"))')
            dbConn.commit()
            self.parent._releaseDBConn()
            try:
                if dropRawData:
                    dbConn.execute('DROP TABLE raw_fmt')
                    dbConn.commit()
            except:
                self.logger.exception('Failed to drop orginal scan data table!')
            return True
        except:
            self.logger.exception('Unknown error occured when aggregate FMT samples.')
            return False
        finally:
            try:
                self.parent._releaseDBConn()
            except:
                pass
            self.logger.info('FMT post import process done.')



#    def __fmtSampleCGIMatch(self):
#        '''
#        Not used any more
#        '''
#        self.logger.info('Start to match FMT scan sample with cell configuration...')
#
#        dbConn = self.parent._acquireDBConn()
#        if not dbConn:
#            self.logger.error('Database connection not created!')
#            self.parent._releaseDBConn()
#            return False
#        try:
#            geoCursor = dbConn.cursor()
#            innerCursor = dbConn.cursor()
#            ciCursor = dbConn.cursor()
#
#            self.logger.info('Matching decoded BSIC with BCCH...')
#            #self.parent.resetProgressBarEvent.set()
#
#            self.logger.info('Start GEO loop')
#            uniqueGeoSql = 'SELECT DISTINCT x,y,longitude,latitude FROM raw_fmt_aggregate WHERE bsic != "NULL"'
#            self.logger.info('SQL shoot')
#            geoCursor.execute(uniqueGeoSql)
#            geoSamples = geoCursor.fetchall()   #Find unique sample coords, if any
#            #for row in geoCursor:
#            rowToSave = 0
#            coordsProceed = 0
#            totalCoords = len(geoSamples)
#            self.logger.info('{} sample coords with BCCH and BSIC need to be matched...'.format(totalCoords))
#            for row in geoSamples:
#                #self.logger.info('Process GEO {} {}'.format(row[0],row[1]))
#                #Find samples on coords
#                GeoNoneBsicScanedSql = 'SELECT x,y,bcch,bsic,rowid FROM raw_fmt_aggregate WHERE bsic != "NULL" AND longitude = ? AND latitude = ?'
#                innerCursor.execute(GeoNoneBsicScanedSql,(row[2],row[3]))
#                for sample in innerCursor:
#                    #Match cell within coords +- step on BCCH and BSIC
#                    cellMatchSql = 'SELECT cgi,x,y,bcch,bsic,( x - ?)*( x - ?) + ( y - ? )*( y -?) as dist2 FROM cellNetworkInfo WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ? AND bcch = ? AND bsic = ? ORDER BY dist2'
#                    step = 500
#                    ciCursor.execute(cellMatchSql,(sample[0],sample[0],sample[1],sample[1],sample[0] - step,sample[0] + step,sample[1] -step ,sample[1] + step,sample[2],sample[3]))
#                    hits = ciCursor.fetchall()
#                    while not hits and step < self.maxBsicMatchRange:
#                        step += 500
#                        ciCursor.execute(cellMatchSql,(sample[0],sample[0],sample[1],sample[1],sample[0] - step,sample[0] + step,sample[1] -step ,sample[1] + step,sample[2],sample[3]))
#                        hits = ciCursor.fetchall()
#                    if hits:
#                        #If any
#                        updateBsicScanedSql = 'UPDATE raw_fmt_aggregate SET cgi = ? ,distance = ? WHERE rowid = ?'
#                        #self.logger.info('{}'.format((hits[0][0],sqrt(hits[0][5]),sample[4])))
#                        try:
#                            ciCursor.execute(updateBsicScanedSql,(hits[0][0],sqrt(hits[0][5]),sample[4]))
#                            rowToSave+=1
#                        except:
#                            self.logger.exception('Fail to update matched sample,rowid {} longitude {} latitude {} bcch {} bsic {} cgi {}'.format(
#                                sample[4],sample[0],sample[1], sample[2],sample[3],hits[0][0]
#                            ))
#                    else:
#                        self.logger.warn('Unable to match BCCH {} BSIC {} at coords ({},{}) within search range {} meters!'.format(sample[2],sample[3],row[2],row[3],self.maxBsicMatchRange))
#                if rowToSave % 10000 == 0:
#                    dbConn.commit()
#                coordsProceed += 1
#                if int(coordsProceed *100 / totalCoords) -  int((coordsProceed-1) *100 / totalCoords) > 0:
#                    #self.parent.tickProgressBarEvent.set()
#                    self.logger.info('Progress {}%'.format( int(coordsProceed *100 / totalCoords)))
#            dbConn.commit()
#            self.logger.info('{} sample coords processed with scaned BCCH and BSIC,done.'.format(coordsProceed))
#
#            #----------------------------RAW BCCH match--------------------------------------------
#            withinRangeRowidSearchCursor = dbConn.cursor()
#            self.logger.info('Matching scaned BCCH without BSIC decoded started...')
#            self.logger.info('Start GEO loop')
#            uniqueGeoSql = 'SELECT DISTINCT x,y,longitude,latitude FROM raw_fmt_aggregate WHERE bsic = "NULL" ORDER BY x,y'
#            self.logger.info('SQL shoot')
#            geoCursor.execute(uniqueGeoSql)
#            geoSamples = geoCursor.fetchall()   #Find unique sample coords, if any
#            #for row in geoCursor:
#            rowToSave = 0
#            coordsProceed = 0
#            totalCoords = len(geoSamples)
#            self.logger.info('{} sample coords without BSIC need to be matched...'.format(totalCoords))
#            dbConn.execute('CREATE TEMP TABLE IF NOT EXISTS preSelect (rowid INTEGER PRIMARY KEY,x REAL,y REAL,bcch INTEGER,bsic TEXT,arxlev REAL,cgi TEXT,dist2 REAL)')
#            dbConn.commit()
#            insertTempTableWithinRangeRowidSearchSql = 'INSERT INTO preSelect SELECT rowid,x,y,bcch,bsic,arxlev,cgi,( x - ?)*( x - ?) + ( y - ? )*( y -?) as dist2 FROM raw_fmt_aggregate WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ? AND bsic !="NULL" AND cgi NOTNULL ORDER BY dist2'
#            for row in geoSamples:
#                #self.logger.info('Process GEO {} {}'.format(row[0],row[1]))
#                #Find samples on coords
#                step = 500
#                dbConn.execute('DELETE FROM preSelect')
#                dbConn.execute(insertTempTableWithinRangeRowidSearchSql,(row[0],row[0],row[1],row[1],row[0] - step,row[0] + step,row[1] -step ,row[1] + step))
#                #dbConn.commit()
#                GeoNoneBsicScanedSql = 'SELECT x,y,bcch,arxlev,rowid FROM raw_fmt_aggregate WHERE bsic = "NULL" AND longitude = {} AND latitude = {}'.format(row[2],row[3])
#                try:
#                    innerCursor.execute(GeoNoneBsicScanedSql)
#                except sqlite3.InterfaceError:
#                    self.logger.exception('Error occured with SQL:{}'.format(GeoNoneBsicScanedSql))
#                    continue
#                toBeUpdated = innerCursor.fetchall()
#                #self.logger.debug('Len of result:{},SQL:{}'.format(len(toBeUpdated),GeoNoneBsicScanedSql))
#                for sample in toBeUpdated:
#                    preMatchedBsicFindInListSql = 'SELECT x,y,bcch,bsic,arxlev,cgi,dist2 FROM preSelect WHERE bcch = ? ORDER BY dist2'
#                    ciCursor.execute(preMatchedBsicFindInListSql,(sample[2],))
#                    hits = ciCursor.fetchall()
#                    while not hits and step < self.maxBcchMatchRange:
#                        step += 500
#                        dbConn.execute('DELETE FROM preSelect')
#                        dbConn.execute(insertTempTableWithinRangeRowidSearchSql,(row[0],row[0],row[1],row[1],row[0] - step,row[0] + step,row[1] -step ,row[1] + step))
#                        #dbConn.commit()
#                        #ciCursor.execute(preMatchedBsicFindSql,(sample[0],sample[0],sample[1],sample[1],sample[0] - step,sample[0] + step,sample[1] -step ,sample[1] + step,sample[2]))
#                        #ciCursor.execute(preMatchedBsicFindInListSql.format(rowidListStr),(sample[0],sample[0],sample[1],sample[1],sample[2]))
#                        ciCursor.execute(preMatchedBsicFindInListSql,(sample[2],))
#                        hits = ciCursor.fetchall()
#                    if hits:
#                        #If any
#                        updateBsicScanedSql = 'UPDATE raw_fmt_aggregate SET cgi = ? ,ref_distance = ? WHERE rowid = ?'
#                        #self.logger.info('{}'.format((hits[0][0],sqrt(hits[0][5]),sample[4])))
#                        try:
#                            ciCursor.execute(updateBsicScanedSql,(hits[0][5],sqrt(hits[0][6]),sample[4]))
#                            rowToSave+=1
#                        except:
#                            self.logger.exception('Fail to update matched sample,rowid {} longitude {} latitude {} bcch {} bsic NULL cgi {}'.format(
#                                sample[4],sample[0],sample[1], sample[2],sample[3],hits[0][5]
#                            ))
#                    else:
#                        #self.logger.info('Unable to match BCCH {} at coords ({},{}) within search range {} meters and matched BSIC history!'.format(sample[2],row[2],row[3],self.maxBcchMatchRange))
#                        pass
#                    if rowToSave % 10000 == 0:
#                        dbConn.commit()
#                coordsProceed += 1
#                if int(coordsProceed *100 / totalCoords) -  int((coordsProceed-1) *100 / totalCoords) > 0:
#                    #self.parent.tickProgressBarEvent.set()
#                    self.logger.info('Progress {}%'.format( int(coordsProceed *100 / totalCoords)))
#            dbConn.commit()
#            self.isMatched = True
#            self.logger.info('{} sample coords processed with scaned BCCH and BSIC,done.'.format(coordsProceed))
#            self.logger.info('Complete all match sample task.')
#            dbConn.execute('DELETE FROM projectinfo WHERE attribute = "dataload" AND value = "scanMatch"')
#            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","scanMatch",datetime("now"))')
#            dbConn.commit()
#            return True
#        except:
#            self.logger.exception('Unexpect error occured during scan sample matching,process aborted!')
#            return False
#        finally:
#            dbConn.commit()
#            self.parent._releaseDBConn()

    def fmtSampleCGIMatchCached(self):
        self.logger.info('Start to match FMT scan sample with cell configuration...')

        dbConn = self.parent._acquireDBConn()
        if not dbConn:
            self.logger.error('Database connection not created!')
            self.parent._releaseDBConn()
            return False
        try:
            geoCursor = dbConn.cursor()
            innerCursor = dbConn.cursor()
            ciCursor = dbConn.cursor()

            self.logger.info('Matching decoded BSIC with BCCH...')
            #self.parent.resetProgressBarEvent.set()

            self.logger.info('Start GEO loop')
            uniqueGeoSql = 'SELECT DISTINCT x,y,longitude,latitude FROM raw_fmt_aggregate WHERE bsic != "NULL"'
            self.logger.info('SQL shoot')
            geoCursor.execute(uniqueGeoSql)
            geoSamples = geoCursor.fetchall()   #Find unique sample coords, if any
            #for row in geoCursor:
            rowToSave = 0
            coordsProceed = 0
            totalCoords = len(geoSamples)
            self.logger.info('{} sample coords with BCCH and BSIC need to be matched...'.format(totalCoords))
            for row in geoSamples:
                #self.logger.info('Process GEO {} {}'.format(row[0],row[1]))
                #Find samples on coords
                GeoNoneBsicScanedSql = 'SELECT x,y,bcch,bsic,rowid FROM raw_fmt_aggregate WHERE bsic != "NULL" AND longitude = ? AND latitude = ?'
                innerCursor.execute(GeoNoneBsicScanedSql,(row[2],row[3]))
                for sample in innerCursor:
                    #Match cell within coords +- step on BCCH and BSIC
                    cellMatchSql = 'SELECT cgi,x,y,bcch,bsic,( x - ?)*( x - ?) + ( y - ? )*( y -?) as dist2 FROM cellNetworkInfo WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ? AND bcch = ? AND bsic = ? ORDER BY dist2'
                    step = 500
                    ciCursor.execute(cellMatchSql,(sample[0],sample[0],sample[1],sample[1],sample[0] - step,sample[0] + step,sample[1] -step ,sample[1] + step,sample[2],sample[3]))
                    hits = ciCursor.fetchall()
                    while not hits and step < self.maxBsicMatchRange:
                        step += 500
                        ciCursor.execute(cellMatchSql,(sample[0],sample[0],sample[1],sample[1],sample[0] - step,sample[0] + step,sample[1] -step ,sample[1] + step,sample[2],sample[3]))
                        hits = ciCursor.fetchall()
                    if hits:
                        #If any
                        updateBsicScanedSql = 'UPDATE raw_fmt_aggregate SET cgi = ? ,distance = ? WHERE rowid = ?'
                        #self.logger.info('{}'.format((hits[0][0],sqrt(hits[0][5]),sample[4])))
                        try:
                            ciCursor.execute(updateBsicScanedSql,(hits[0][0],sqrt(hits[0][5]),sample[4]))
                            rowToSave+=1
                        except:
                            self.logger.exception('Fail to update matched sample,rowid {} longitude {} latitude {} bcch {} bsic {} cgi {}'.format(
                                sample[4],sample[0],sample[1], sample[2],sample[3],hits[0][0]
                            ))
                    else:
                        self.logger.warn('Unable to match BCCH {} BSIC {} at coords ({},{}) within search range {} meters!'.format(sample[2],sample[3],row[2],row[3],self.maxBsicMatchRange))
                if rowToSave % 10000 == 0:
                    dbConn.commit()
                coordsProceed += 1
                if int(coordsProceed *100 / totalCoords) -  int((coordsProceed-1) *100 / totalCoords) > 0:
                    #self.parent.tickProgressBarEvent.set()
                    self.logger.info('Progress {}%'.format( int(coordsProceed *100 / totalCoords)))
            dbConn.commit()
            self.logger.info('{} sample coords processed with scaned BCCH and BSIC,done.'.format(coordsProceed))

            #----------------------------RAW BCCH match--------------------------------------------
            withinRangeRowidSearchCursor = dbConn.cursor()
            self.logger.info('Matching scaned BCCH without BSIC decoded started...')
            self.logger.info('Start GEO loop')
            uniqueGeoSql = 'SELECT DISTINCT x,y,longitude,latitude FROM raw_fmt_aggregate WHERE bsic = "NULL" ORDER BY x,y'
            self.logger.info('SQL shoot')
            geoCursor.execute(uniqueGeoSql)
            geoSamples = geoCursor.fetchall()   #Find unique sample coords, if any
            #for row in geoCursor:
            rowToSave = 0
            coordsProceed = 0
            totalCoords = len(geoSamples)
            self.logger.info('{} sample coords without BSIC need to be matched...'.format(totalCoords))
            cacheCursor = dbConn.cursor()
            cacheSql = 'SELECT rowid,x,y,bcch,bsic,arxlev,cgi,( x - ?)*( x - ?) + ( y - ? )*( y -?) as dist2 FROM raw_fmt_aggregate WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ? AND bsic !="NULL" AND cgi NOTNULL ORDER BY dist2'
            for row in geoSamples:
                #self.logger.info('Process GEO {} {}'.format(row[0],row[1]))
                #Find samples on coords
                step = 100
                cacheCursor.execute(cacheSql,(row[0],row[0],row[1],row[1],row[0] - step,row[0] + step,row[1] -step ,row[1] + step))
                cacheResult = cacheCursor.fetchall()
                cacheDateSet = {}
                for crow in cacheResult:
                    if crow[3] and not cacheDateSet.get(crow[3]):
                        cacheDateSet[crow[3]] = (crow[6],sqrt(crow[7]))
                GeoNoneBsicScanedSql = 'SELECT x,y,bcch,arxlev,rowid FROM raw_fmt_aggregate WHERE bsic = "NULL" AND longitude = {} AND latitude = {}'.format(row[2],row[3])
                try:
                    innerCursor.execute(GeoNoneBsicScanedSql)
                except sqlite3.InterfaceError:
                    self.logger.exception('Error occured with SQL:{}'.format(GeoNoneBsicScanedSql))
                    continue
                toBeUpdated = innerCursor.fetchall()
                #self.logger.debug('Len of result:{},SQL:{}'.format(len(toBeUpdated),GeoNoneBsicScanedSql))
                for sample in toBeUpdated:
                    #self.logger.debug('sample[2]:{},dataSet {}'.format(sample[2],cacheDateSet))
                    while not cacheDateSet.get(sample[2]) and step < self.maxBcchMatchRange:
                        step += step
                        cacheCursor.execute(cacheSql,(row[0],row[0],row[1],row[1],row[0] - step,row[0] + step,row[1] -step ,row[1] + step))
                        cacheResult = cacheCursor.fetchall()
                        cacheDateSet = {}
                        for crow in cacheResult:
                            if crow[3] and not cacheDateSet.get(crow[3]):
                                cacheDateSet[crow[3]] = (crow[6],sqrt(crow[7]))
                    if cacheDateSet.get(sample[2]):
                        #If any
                        updateBsicScanedSql = 'UPDATE raw_fmt_aggregate SET cgi = ? ,ref_distance = ? WHERE rowid = ?'
                        #self.logger.info('{}'.format((hits[0][0],sqrt(hits[0][5]),sample[4])))
                        try:
                            ciCursor.execute(updateBsicScanedSql,(cacheDateSet.get(sample[2])[0],cacheDateSet.get(sample[2])[1],sample[4]))
                            rowToSave+=1
                        except:
                            self.logger.exception('Fail to update matched sample,rowid {} longitude {} latitude {} bcch {} bsic NULL cgi {}'.format(
                                sample[4],sample[0],sample[1], sample[2],sample[3],cacheDateSet.get(sample[2])[0]
                            ))
                    else:
                        #self.logger.info('Unable to match BCCH {} at coords ({},{}) within search range {} meters and matched BSIC history!'.format(sample[2],row[2],row[3],self.maxBcchMatchRange))
                        pass
                    if rowToSave % 10000 == 0:
                        dbConn.commit()
                coordsProceed += 1
                if int(coordsProceed *100 / totalCoords) -  int((coordsProceed-1) *100 / totalCoords) > 0:
                    #self.parent.tickProgressBarEvent.set()
                    self.logger.info('Progress {}%'.format( int(coordsProceed *100 / totalCoords)))
            dbConn.commit()
            self.isMatched = True
            self.logger.info('{} sample coords processed with scaned BCCH and BSIC,done.'.format(coordsProceed))
            self.logger.info('Complete all match sample task.')
            dbConn.execute('DELETE FROM projectinfo WHERE attribute = "dataload" AND value = "scanMatch"')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","scanMatch",datetime("now"))')
            dbConn.commit()
            return True
        except:
            self.logger.exception('Unexpect error occured during scan sample matching,process aborted!')
            return False
        finally:
            dbConn.commit()
            self.parent._releaseDBConn()

class NSAnalyzerClass(object):
    arfcnlist_900 = range(1,10)+range(11,20)+range(21,30)+range(31,40)+range(41,50)+range(51,72)
    arfcnlist_1800 = range(536,590)+range(591,600)+range(601,610)+range(611,620)+range(621,637)
    MAX_ERLperKm2 = {20:
                         {300:1072,
                          350:787,
                          400:602,
                          450:476,
                          500:387,
                          550:319,
                          600:268,
                          800:150,
                          1000:96},
                     25:
                         {300:1377,
                          350:1012,
                          400:755,
                          450:612,
                          500:496,
                          550:410,
                          600:344,
                          800:194,
                          1000:124}
    }
    MAX_BAND_WIDTH = 20
    NEI_RESOLVE_MAX_DEPTH = 5 #未定义邻区最大匹配深度
    MAX_CONFLICT_RESOLVE_DEPTH = 5 #频点退让最大迭代深度
    CORR_THRES =0.03
    OVERLAP_COR_THRES = 0.03 # = CORR_THRES #判断干扰小区越区覆盖的条件之一，服务小区电平低于邻小区12dB的测量报告比例
    OVERLAP_COVER_THRES = 0.005 #干扰小区在服务小区测量报告中的占比
    ValidXmin = 73
    ValidXmax = 135
    ValidYmin = 3
    ValidYmax = 53
    GRID_SIZE = 200 #500m
    
    def __init__(self,parent = None):
        self.parent = parent

        self.FailedTryCellSets = [] #当前小区频点退让过程中已经处理失败的小区对
        self.ServCellMR = {} #统计小区话务量，除以６为真正的测量报告数
        self.CELLINFO = {} #小区原始工参
        self.GCELL = {}  #小区索引
        self.RAW_MR = {} #原始S36x系列测量报告,先占个位
        self.matchedMR = {} #匹配后的测量报告
    
        self.ARFCN_IDX = {} #频点索引
        self.BCCH_BSIC_IDX = {} #BCCH-BSIC索引
        self.G2GNCELL = {} #邻区信息
    
        self.CorrelateClusters = [] #最大连通簇，不再使用
        self.hashedMaximalConnClusters = {} #最大连通簇
        self.ClusterAttribute = {} #连通簇属性
        self.CGI2Clusters = {} #小区到联通簇的索引，不再使用
        self.CGI2HashedClusters = {} #小区到连通簇的索引
        self.CorrelateMatrixBy12dB = {} #小区干扰矩阵，12dB门限，可仅基于同频
        self.CorrelateMatrixByAppearance = {} #小区间可见关系，跟是否同频无关
        self.CellCoverageInfo = {} #Based on 12dB MRs
    
        self.EscapedWarn = [] #需要屏蔽的告警信息
    
        self.GEOSets = {}    #保存站址信息
        self.CGI2GEO = {}
        self.CGICoverageBoundary = {}
        self.CGICoverageGridsByTA = {} #小区在不同的TA范围内所覆盖的网格数
        self.CGIErlWeightsByTaGrids = {} #小区在不同TA范围下的每网格所占话务权重
    
        self.GridMatrix = {}
        self.GridSiteDist = {}
        self.GridAttributes = {}
    
        self.CGITraffic_TA = {}  #小区话务量及TA分布
    
        self.CGIPerformance = {}
        self.CGIPerformanceKey = []

        self.createCellNetworkInfoTableSql = 'CREATE TABLE IF NOT EXISTS cellNetworkInfo (cgi TEXT PRIMARY KEY,cellname TEXT,bcch INTEGER, bsic TEXT, longitude REAL,latitude REAL,x REAL,y REAL,projx REAL,projy REAL,bscname TEXT,dir REAL,tile REAL,height REAL,coverage_type TEXT,extcell INTEGER,tch_count INTEGER)'
        self.createCellTrxTableSql = 'CREATE TABLE IF NOT EXISTS cellTrx (cgi TEXT ,arfcn INTEGER,type TEXT)'
        self.createBlackMrTableSql = 'CREATE TABLE IF NOT EXISTS blackMR (scgi TEXT ,blackID TEXT,blackMrs INTEGER,serverMRs REAL)'
        self.createCellNetworkInfoIndexX = 'CREATE INDEX IF NOT EXISTS cell_x on cellNetworkInfo (x ASC)'
        self.createCellNetworkInfoIndexY = 'CREATE INDEX IF NOT EXISTS cell_y on cellNetworkInfo (y ASC)'
        self.createCellNetworkInfoIndexXY = 'CREATE INDEX IF NOT EXISTS cell_xy on cellNetworkInfo (x ASC,y ASC)'
        self.dropCellNetworkInfoIndexX = 'DROP INDEX IF EXISTS cell_x'
        self.dropCellNetworkInfoIndexY = 'DROP INDEX IF EXISTS cell_y'
        self.dropCellNetworkInfoIndexXY = 'DROP INDEX IF EXISTS cell_xy'

        self.logger = logging.getLogger('global')
        self.progressQueue = _globals.getProgressQueue()

        self.isMrMatched = False #if MR is matched set it to True
        self.isMrPostProcessed = False #if reIndexGcellRelation runned
        self.isMaximalConnectClusterDetected = False
        self.maximalConnectClusterDetected3 = False #Only for Detect3 function
        self.isCellMrInfoCalced = False #calc cell coverage interference by MR
        self.isBlackMrFitered = False

    def GenRFPlan(self):
        plan = {}
        for CGI in self.GCELL:
            plan[CGI] = [self.GCELL[CGI].get('BCCH','Unknown')]+[arfcn for arfcn in self.GCELL[CGI].get('TRX',{}).keys() if arfcn != self.GCELL[CGI].get('BCCH') ]

        return plan


    def CellPerformanceParser(self,filePath):
        if filePath:
            #print('{} started...'.format('CellPerformaceParser'))
            self.logger.info('{} started...'.format('CellPerformaceParser'))
            with open(filePath,'rb') as fp:
                reader = csv.reader(fp)
                header = reader.next()
                idx_cgi = header.index('CGI')

                idx_x = {}
                for key in header:
                    if key == 'CGI':
                        continue
                    idx_x[key] = header.index(key)
                    self.CGIPerformanceKey.append(key)

                for row in reader:
                    self.CGIPerformance[row[idx_cgi]] = {}
                    for key in idx_x:
                        try:
                            self.CGIPerformance[row[idx_cgi]][key] = float(row[idx_x[key]])
                        except ValueError:
                            #print('CGI {} failed to parser {}'.format(row[idx_cgi],key))
                            self.logger.warn('CGI {} failed to parser {}'.format(row[idx_cgi],key).decode('gbk'))
                #print('{} done.'.format('CellPerformaceParser'))
            self.logger.info('{} done.'.format('CellPerformaceParser'))


    def _ResetMMLDataSet(self):
        self.CELLINFO = {}
        self.ARFCN_IDX = {}
        self.BCCH_BSIC_IDX = {}
        self.GCELL = {}
        self.G2GNCELL = {}
        self.CGI2HashedClusters = {}
        self.hashedMaximalConnClusters = {}
        self.isMrMatched = False
        self.isBlackMrFitered = False
        self.isMrPostProcessed = False
        self.isMaximalConnectClusterDetected = False
        self.isCellMrInfoCalced = False

#        dbConn = self.parent._acquireDBConn()
#        if not dbConn:
#            self.parent._releaseDBConn()
#        else:
#            try:
#                dbConn.execute('DROP TABLE IF EXISTS cellNetworkInfo')
#            except:
#                pass
#            finally:
#                    self.parent._releaseDBConn()

    def _ResetGeoCfgDataSet(self):
        self.GEOSets = {}
        self.CGI2GEO = {}
        try:
            self.__delattr__('Xmax')
        except AttributeError:
            pass
        try:
            self.__delattr__('Xmin')
        except AttributeError:
            pass
        try:
            self.__delattr__('Ymax')
        except AttributeError:
            pass
        try:
            self.__delattr__('Ymin')
        except AttributeError:
            pass

#        dbConn = self.parent._acquireDBConn()
#        if not dbConn:
#            self.parent._releaseDBConn()
#        else:
#            try:
#                dbConn.execute('DROP TABLE IF EXISTS cellNetworkInfo')
#            except:
#                pass
#            finally:
#                self.parent._releaseDBConn()

    def _ResetMRDataSet(self):
        self.RAW_MR = {}
        self.ServCellMR = {}
        self.matchedMR = {}
        self.CGI2HashedClusters = {}
        self.hashedMaximalConnClusters = {}
        self.isMrMatched = False
        self.isBlackMrFitered = False
        self.isMrPostProcessed = False
        self.isMaximalConnectClusterDetected = False
        self.isCellMrInfoCalced = False

    def _ResetCellPerformanceDataSet(self):
        self.CGIPerformance = {}

    def _ResetCellTrafficDataSet(self):
        self.CGITraffic_TA = {}

    def _ResetDependencyDataSet(self):
        pass

    def MMLPaser(self,filePath):
        #
        def GTRXParser(filePath):
            self.logger.info('Start parser ADD GTRX.csv...')

            if filePath:
                pth = os.path.join(filePath,'ADD GTRX.csv')
                fp = open(pth,'rb')
                reader = csv.reader(fp)
                header = reader.next()
                idx_file = header.index('FILE')
                idx_trxid = header.index('TRXID')
                idx_freq = header.index('FREQ')
                idx_cid = header.index('CELLID')
                idx_bcch = header.index('ISMAINBCCH')

                for row in reader:
                    if self.CELLINFO.get(row[idx_file]):
                        if self.CELLINFO[row[idx_file]].get(row[idx_cid]):
                            try:
                                self.CELLINFO[row[idx_file]][row[idx_cid]]['TRX'][row[idx_freq]] = row[idx_trxid]
                            except:
                                self.CELLINFO[row[idx_file]][row[idx_cid]]['TRX'] = {}
                                self.CELLINFO[row[idx_file]][row[idx_cid]]['TRX'][row[idx_freq]] = row[idx_trxid]

                            try:
                                self.ARFCN_IDX[row[idx_freq]].append(self.CELLINFO[row[idx_file]][row[idx_cid]]['CGI'])
                            except :
                                self.ARFCN_IDX[row[idx_freq]]=[]
                                self.ARFCN_IDX[row[idx_freq]].append(self.CELLINFO[row[idx_file]][row[idx_cid]]['CGI'])

                            if  'YES' == row[idx_bcch].strip().upper():
                                #Found BCCH TRX
                                self.CELLINFO[row[idx_file]][row[idx_cid]]['BCCH'] = row[idx_freq]
                                try:
                                    self.BCCH_BSIC_IDX['{}#{}#{}'.format(row[idx_freq],self.CELLINFO[row[idx_file]][row[idx_cid]]['NCC'],self.CELLINFO[row[idx_file]][row[idx_cid]]['BCC'])].add(self.CELLINFO[row[idx_file]][row[idx_cid]]['CGI'])
                                except :
                                    self.BCCH_BSIC_IDX['{}#{}#{}'.format(row[idx_freq],self.CELLINFO[row[idx_file]][row[idx_cid]]['NCC'],self.CELLINFO[row[idx_file]][row[idx_cid]]['BCC'])] = set([])
                                    self.BCCH_BSIC_IDX['{}#{}#{}'.format(row[idx_freq],self.CELLINFO[row[idx_file]][row[idx_cid]]['NCC'],self.CELLINFO[row[idx_file]][row[idx_cid]]['BCC'])].add(self.CELLINFO[row[idx_file]][row[idx_cid]]['CGI'])
                        else:
                            self.logger.error('Can not found {} in file {}'.format(row[idx_cid],row[idx_file]))
                    else:
                        self.logger.error('Can not found file {}'.format(row[idx_file]))
                fp.close()
                self.logger.info('TRX proceed')
            else:
                self.logger.warn('Invalid GTRX filename!')

        def GCELLParser(filePath):
            self.logger.info('Start parser ADD GCELL.csv...')
            if filePath:
                pth = os.path.join(filePath,'ADD GCELL.csv')
                fp = open(pth,'rb')
                reader = csv.reader(fp)
                header = reader.next()
                idx_file = header.index('FILE')
                idx_cname = header.index('CELLNAME')
                idx_cid = header.index('CELLID')
                idx_MCC = header.index('MCC')
                idx_MNC = header.index('MNC')
                idx_LAC = header.index('LAC')
                idx_CI = header.index('CI')
                idx_NCC = header.index('NCC')
                idx_BCC = header.index('BCC')

                for row in reader:
                    CID = row[idx_cid]
                    CGI = '{}-{}-{}-{}'.format(row[idx_MCC].replace('"',''),row[idx_MNC].replace('"',''),row[idx_LAC],row[idx_CI])
                    try:
                        self.CELLINFO[row[idx_file]][CID] = {}
                        self.CELLINFO[row[idx_file]][CID]['NCC'] = row[idx_NCC]
                        self.CELLINFO[row[idx_file]][CID]['BCC'] = row[idx_BCC]
                        self.CELLINFO[row[idx_file]][CID]['NAME'] = row[idx_cname].replace('"','')
                        self.CELLINFO[row[idx_file]][CID]['CGI'] = CGI
                    except :
                        self.CELLINFO[row[idx_file]] = {}
                        self.CELLINFO[row[idx_file]][CID] = {}
                        self.CELLINFO[row[idx_file]][CID]['NCC'] = row[idx_NCC]
                        self.CELLINFO[row[idx_file]][CID]['BCC'] = row[idx_BCC]
                        self.CELLINFO[row[idx_file]][CID]['NAME'] = row[idx_cname]
                        self.CELLINFO[row[idx_file]][CID]['CGI'] = CGI
                    self.GCELL[CGI] = self.CELLINFO[row[idx_file]][CID]
                fp.close()
                self.logger.info('ADD GCELL proceed')
            else:
                self.logger.warn('Invalid ADD GCELL filename!')

        def GEXT2GCELLPaser(filePath):
            self.logger.info('Start parser ADD GEXT2GCELL.csv...')
            if filePath:
                pth = os.path.join(filePath,'ADD GEXT2GCELL.csv')
                fp = open(pth,'rb')
                reader = csv.reader(fp)
                header = reader.next()
                idx_file = header.index('FILE')
                idx_cname = header.index('EXT2GCELLNAME')
                idx_cid = header.index('EXT2GCELLID')
                idx_MCC = header.index('MCC')
                idx_MNC = header.index('MNC')
                idx_LAC = header.index('LAC')
                idx_CI = header.index('CI')
                idx_NCC = header.index('NCC')
                idx_BCC = header.index('BCC')
                idx_BCCH = header.index('BCCH')

                for row in reader:
                    CID = row[idx_cid]
                    #CGI = '{}-{}-{}-{}'.format(row[idx_MCC],row[idx_MNC],row[idx_LAC],row[idx_CI])
                    CGI = '{}-{}-{}-{}'.format(row[idx_MCC].replace('"',''),row[idx_MNC].replace('"',''),row[idx_LAC],row[idx_CI])
                    try:
                        self.CELLINFO[row[idx_file]][CID] = {}
                    except :
                        self.CELLINFO[row[idx_file]] = {}
                        self.CELLINFO[row[idx_file]][CID] = {}
                    finally:
                        self.CELLINFO[row[idx_file]][CID]['NCC'] = row[idx_NCC]
                        self.CELLINFO[row[idx_file]][CID]['BCC'] = row[idx_BCC]
                        self.CELLINFO[row[idx_file]][CID]['NAME'] = row[idx_cname].replace('"','')
                        self.CELLINFO[row[idx_file]][CID]['CGI'] = CGI
                        self.CELLINFO[row[idx_file]][CID]['BCCH'] = row[idx_BCCH]
                        self.CELLINFO[row[idx_file]][CID]['EXTCELL'] = True
                        self.CELLINFO[row[idx_file]][CID]['TRX'] = {row[idx_BCCH]:-1}

                        if CGI in self.GCELL:
                            if '{}#{}#{}'.format(self.GCELL[CGI]['BCCH'],self.GCELL[CGI]['NCC'],self.GCELL[CGI]['BCC']) != '{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC]):
                                if self.GCELL[CGI].get('EXTCELL'):
                                    #print('Found invalid EXT-CGI {} with {},suggestion {}'.format(CGI,'{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC]),'{}#{}#{}'.format(GCELL[CGI]['BCCH'],GCELL[CGI]['NCC'],GCELL[CGI]['BCC'])))
                                    self.logger.warn('Found invalid EXT-CGI {} with {},suggestion {}'.format(CGI,'{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC]),'{}#{}#{}'.format(self.GCELL[CGI]['BCCH'],self.GCELL[CGI]['NCC'],self.GCELL[CGI]['BCC'])))
                                else:
                                    #print('Found invalid EXT-CGI {} with {},correction {}'.format(CGI,'{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC]),'{}#{}#{}'.format(GCELL[CGI]['BCCH'],GCELL[CGI]['NCC'],GCELL[CGI]['BCC'])))
                                    self.logger.warn('Found invalid EXT-CGI {} with {},correction {}'.format(CGI,'{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC]),'{}#{}#{}'.format(self.GCELL[CGI]['BCCH'],self.GCELL[CGI]['NCC'],self.GCELL[CGI]['BCC'])))
                        else:
                            try:
                                self.BCCH_BSIC_IDX['{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC])].add(CGI)
                            except :
                                self.BCCH_BSIC_IDX['{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC])] = set([])
                                self.BCCH_BSIC_IDX['{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC])].add(CGI)
                    if not self.GCELL.has_key(CGI):
                        self.GCELL[CGI] = self.CELLINFO[row[idx_file]][CID]
                fp.close()
                self.logger.info('ADD GEXT2GCELL proceed')
            else:
                self.logger.warn('Invalid ADD GEXT2GCELL filename!')

        def G2GNCELLPaser(filePath):
            self.logger.info('Start parser ADD G2GNCELL.csv...')
            if filePath:
                pth = os.path.join(filePath,'ADD G2GNCELL.csv')
                fp = open(pth,'rb')
                reader = csv.reader(fp)
                header = reader.next()
                idx_file = header.index('FILE')
                idx_sid = header.index('SRC2GNCELLID')
                idx_nid = header.index('NBR2GNCELLID')

                for row in reader:
                    if row[idx_file] not in self.CELLINFO:
                        self.logger.error('Unexpected FILE {} in G2GNCELL'.format(row[idx_file]))
                        continue

                    if row[idx_sid] in  self.CELLINFO[row[idx_file]] and row[idx_nid] in self.CELLINFO[row[idx_file]]:
                        try:
                            if self.CELLINFO[row[idx_file]][row[idx_sid]].get('CGI') and self.CELLINFO[row[idx_file]][row[idx_nid]].get('CGI'):
                                self.G2GNCELL[self.CELLINFO[row[idx_file]][row[idx_sid]]['CGI']].append(self.CELLINFO[row[idx_file]][row[idx_nid]]['CGI'])
                            else:
                                self.logger.error('Missing CGI in FILE {} for SID {} or NID'.format(row[idx_file],row[idx_sid],row[idx_nid]))
                        except :
                            if self.CELLINFO[row[idx_file]][row[idx_sid]].get('CGI') and self.CELLINFO[row[idx_file]][row[idx_nid]].get('CGI'):
                                self.G2GNCELL[self.CELLINFO[row[idx_file]][row[idx_sid]]['CGI']] = [self.CELLINFO[row[idx_file]][row[idx_nid]]['CGI'],]
                            else:
                                self.logger.error('Missing CGI in FILE {} for SID {} or NID'.format(row[idx_file],row[idx_sid],row[idx_nid]))
                fp.close()
                self.logger.info('ADD G2GNCELL proceed')
            else:
                self.logger.warn('Invalid ADD G2GNCELL filename!')

        self.logger.info('{} started....'.format('MMLPaser'))
        GCELLParser(filePath)
        GTRXParser(filePath)
        GEXT2GCELLPaser(filePath)
        G2GNCELLPaser(filePath)
        self.logger.info('{} done.'.format('MMLPaser'))

        self.logger.info('Saving MML into project db...')
        dbConn = self.parent._acquireDBConn()
        if not dbConn:
            self.logger.error('Database connection not created!')
            self.parent._releaseDBConn()
            return False
        try:
            #dbConn.execute('DROP TABLE IF EXISTS cellNetworkInfo')
            dbConn.execute('DROP TABLE IF EXISTS cellTrx')
            dbConn.execute(self.createCellNetworkInfoTableSql)
            dbConn.execute(self.createCellTrxTableSql)
            dbConn.commit()
            for item in self.GCELL.itervalues():
                try:
                    sql = 'INSERT INTO cellNetworkInfo (cgi,cellname,bcch,bsic,extcell,tch_count) VALUES ("{}","{}",{},"{}",{},{})'.format(item['CGI'],item['NAME'].replace('"','').decode('gbk').encode('utf-8'),item['BCCH'],'{}{}'.format(item['NCC'],item['BCC']),1 if item.get('EXTCELL',None) else 0, len(item['TRX']) -1 )
                    dbConn.execute(sql)
                except sqlite3.IntegrityError:
                    try:
                        sql = 'UPDATE cellNetworkInfo SET cellname = "{}" , bcch = {} , bsic = "{}", extcell = {}, tch_count = {} WHERE CGI = "{}"'.format(item['NAME'].replace('"','').decode('gbk').encode('utf-8'),item['BCCH'],'{}{}'.format(item['NCC'],item['BCC']),1 if item.get('EXTCELL',None) else 0,len(item['TRX']) -1 ,item['CGI'])
                        dbConn.execute(sql)
                    except:
                        self.logger.exception('Fail to update cell basic information,SQL: {}'.format(sql) )
                except sqlite3.OperationalError:
                    self.logger.exception('Update cell basic information error.Syntax error,SQL: {}'.format(sql))

                try:
                    sql = 'INSERT INTO cellTrx (cgi,arfcn,type) VALUES (?,?,?)'
                    dataSet = [ (item['CGI'],arfcn,'NonBCCH') for arfcn in item['TRX'].keys() if arfcn != item['BCCH'] ]
                    dataSet.append((item['CGI'],item['BCCH'],'BCCH'))
                    dbConn.executemany(sql,dataSet)
                except sqlite3.IntegrityError:
                    self.logger.exception('CGI {} duplicate ARFCN found in TRX table or database already imported.'.format(item['CGI']))

            dbConn.commit()
            dbConn.execute('DELETE FROM projectinfo WHERE attribute = "dataload" AND value = "MML"')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","MML",datetime("now"))')
            dbConn.commit()

            return True
        except:
            self.logger.exception('Failed to update cell basic information,unknown error.')
            return False
        finally:
            try:
                self.parent._releaseDBConn()
            except:
                pass

    def GeoCfgParser(self,filePath,zone = 50):
        '''

        '''
        #global Xmax,Xmin,Ymax,Ymin
        self.logger.info('{} started....'.format('GeoCfgParser'))
        #TODO FIX ME update project system
        #Proj = pyproj.Proj(proj='utm',zone = zone,ellps='WGS84')
        self.realProj = _globals.getCalcProj()
        self.mapProj = _globals.getViewerProj()
        #Proj = pyproj.Proj(proj='merc',ellps='WGS84')

        if filePath:
            try:
                with open(filePath,'rb') as fp:
                    reader = csv.reader(fp)
                    header = reader.next()
                    idx_cgi = header.index('CGI')
                    idx_long = header.index('LONGITUDE')
                    idx_lat = header.index('LATITUDE')
                    idx_dir = header.index('DIR')
                    idx_type = header.index('CELLTYPE')
                    idx_height = header.index('HEIGHT')
                    idx_tile = header.index('TILE')

                    for row in reader:
                        cgi = row[idx_cgi]
                        if not cgi.strip():
                            self.logger.warn('None empty CGI allowed. cell escaped')
                            continue
                        try:
                            longi = float(row[idx_long].strip())
                            lat = float(row[idx_lat].strip())
                        except ValueError:
                            self.logger.warn('CGI {} with invalid longitude or latitude {},{}. cell escaped'.format(cgi,row[idx_long],row[idx_lat]))
                            continue

                        celltype = row[idx_type].strip().lower()
                        if celltype not in ['indoor','underlayer','macro']:
                            self.logger.warn('CGI {} with inapporiate coverage type [{}]. cell import escaped'.format(cgi,celltype))
                            continue

                        try:
                            dir = float(row[idx_dir])
                        except :
                            self.logger.warn('CGI {} with inapporiate dir {}.'.format(cgi,row[idx_dir]))
                            #dir = -1
                            dir = -100

                        try:
                            tile = float(row[idx_tile])
                        except :
                            self.logger.warn('CGI {} with inapporiate tile {}.'.format(cgi,row[idx_tile]))
                            tile = -100

                        try:
                            height = float(row[idx_height])
                        except :
                            self.logger.warn('CGI {} with inapporiate height {}.'.format(cgi,row[idx_height]))
                            height = -100

                        InvalidGeo = False
                        try:
                            self.GEOSets[(longi,lat)]['cell'][cgi] = {'type':celltype,'dir':dir,'tile':tile,'height':height}
                        except KeyError:
                            if self.ValidXmin <= longi <= self.ValidXmax and  self.ValidYmin <= lat <= self.ValidYmax:
                                self.GEOSets[(longi,lat)] = {}
                                self.GEOSets[(longi,lat)]['cell'] = {}
                                #self.GEOSets[(longi,lat)]['cell'][cgi] = {'type':celltype,'dir':dir}
                                self.GEOSets[(longi,lat)]['cell'][cgi] = {'type':celltype,'dir':dir,'tile':tile,'height':height}

                                self.GEOSets[(longi,lat)]['CoverageType'] = set()
                                self.GEOSets[(longi,lat)]['coords'] = self.realProj(longi,lat)
                                self.GEOSets[(longi,lat)]['mapCoords'] = self.mapProj(longi,lat)
                            else:
                                self.logger.error('CGI {} has invalid longitude/latitude ,{}/{}.'.format(cgi,longi,lat))
                                InvalidGeo = True
                                continue
                        finally:
                            if not InvalidGeo:
                                self.GEOSets[(longi,lat)]['CoverageType'].add(celltype)
                                self.CGI2GEO[cgi] = (longi,lat)

                                try:
                                    self.Xmax = max(int(self.GEOSets[(longi,lat)]['coords'][0]),self.Xmax)
                                except AttributeError:
                                    self.Xmax = int(self.GEOSets[(longi,lat)]['coords'][0])

                                try:
                                    self.Xmin = min(int(self.GEOSets[(longi,lat)]['coords'][0]),self.Xmin)
                                except AttributeError:
                                    self.Xmin = int(self.GEOSets[(longi,lat)]['coords'][0])

                                try:
                                    self.Ymax = max(int(self.GEOSets[(longi,lat)]['coords'][1]),self.Ymax)
                                except AttributeError:
                                    self.Ymax = int(self.GEOSets[(longi,lat)]['coords'][1])

                                try:
                                    self.Ymin = min(int(self.GEOSets[(longi,lat)]['coords'][1]),self.Ymin)
                                except AttributeError:
                                    self.Ymin = int(self.GEOSets[(longi,lat)]['coords'][1])

                self.logger.info('GEO Parser Done,{} cell added'.format(len(self.CGI2GEO)))
                #---------------------DB saving part-------------
                self.logger.info('Saving to project...')
                dbConn = self.parent._acquireDBConn()
                if not dbConn:
                    self.logger.error('Database connection not created!')
                    self.parent._releaseDBConn()
                    return False
                try:
                    #dbConn.execute('DROP TABLE IF EXISTS cellNetworkInfo')
                    #self.cellNetworkInfoTableSql = 'CREATE TABLE IF NOT EXISTS cellNetworkInfo (cgi TEXT PRIMARY KEY,cellname TEXT,bcch INTEGER, bsic TEXT, longitude REAL,latitude REAL,x REAL,y REAL,bscname TEXT,dir REAL,tile REAL,celltype TEXT)'
                    dbConn.execute(self.createCellNetworkInfoTableSql)
                    dbConn.commit()
                    dbConn.execute(self.dropCellNetworkInfoIndexX)
                    dbConn.execute(self.dropCellNetworkInfoIndexY)
                    dbConn.execute(self.dropCellNetworkInfoIndexXY)
                    for geo in self.GEOSets:
                        for CGI in self.GEOSets[geo]['cell']:
                            #projCoords =
                            try:
                                sql = 'INSERT INTO cellNetworkInfo (cgi,longitude,latitude,x,y,dir,coverage_type,projx,projy,tile,height) VALUES ("{}",{},{},{},{},{},"{}",{},{},{},{})'.format(
                                    CGI,geo[0],geo[1],self.GEOSets[geo]['coords'][0],self.GEOSets[geo]['coords'][1],self.GEOSets[geo]['cell'][CGI]['dir'],self.GEOSets[geo]['cell'][CGI]['type'],self.GEOSets[geo]['mapCoords'][0],self.GEOSets[geo]['mapCoords'][1],self.GEOSets[geo]['cell'][CGI]['tile'],self.GEOSets[geo]['cell'][CGI]['height'])
                                dbConn.execute(sql)
                            except sqlite3.IntegrityError:
                                try:
                                    sql = 'UPDATE cellNetworkInfo SET longitude = {} , latitude = {} , x = {}, y = {}, dir = {}, coverage_type = "{}" , projx = {},projy = {}, tile = {}, height = {} WHERE CGI = "{}"'.format(
                                        geo[0],geo[1],self.GEOSets[geo]['coords'][0],self.GEOSets[geo]['coords'][1],self.GEOSets[geo]['cell'][CGI]['dir'],self.GEOSets[geo]['cell'][CGI]['type'],self.GEOSets[geo]['mapCoords'][0],self.GEOSets[geo]['mapCoords'][1],self.GEOSets[geo]['cell'][CGI]['tile'],self.GEOSets[geo]['cell'][CGI]['height'],CGI)
                                    dbConn.execute(sql)
                                except:
                                    self.logger.exception('Fail to update cell basic information,SQL: {}'.format(sql) )
                            except sqlite3.OperationalError:
                                self.logger.exception('Update cell basic information error.Syntax error,SQL: {}'.format(sql))
                    dbConn.commit()
                    self.logger.info('Saving done.')
                    dbConn.execute(self.createCellNetworkInfoIndexX)
                    dbConn.execute(self.createCellNetworkInfoIndexY)
                    dbConn.execute(self.createCellNetworkInfoIndexXY)
                    dbConn.commit()
                    dbConn.execute('DELETE FROM projectinfo WHERE attribute = "dataload" AND value = "geoInfo"')
                    dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","geoInfo",datetime("now"))')
                    dbConn.commit()
                    return True
                except:
                    self.logger.exception('Failed to update cell basic information,unknown error.')
                    self.logger.info('Saving failed.')
                    return False
                finally:
                    try:
                        self.parent._releaseDBConn()
                    except:
                        pass
            except:
                self.logger.exception(u'Unexpected error encountered when parsering cell geo information data file {}!'.format(filePath))
        else:
            self.logger.error('Missing filepath!')


    def CalcCellCoverageBoundary(self,HalfCoverage = True):
        '''
        计算小区覆盖边界，根据邻区中服务小区测量报告出现的比例和电平
        '''
        CGICount = 0
        x = 10
        #print('{} started....'.format('CalcCellCoverageBoundary'))
        self.logger.info('{} started....'.format('CalcCellCoverageBoundary'))
        for CGI in self.GCELL:
            if self.GCELL[CGI].get('EXTCELL'):
                '''EXT CELL will not be considered'''
                continue
            if CGI not in self.CorrelateMatrixByAppearance:
                self.logger.info('CGI {} can not been seen in MR reports,cell coverage boundary use 0 TA coverage'.format(CGI))
                self.UpdateCoverageByTa0(CGI)
                continue

            self.CGICoverageBoundary[CGI] = set()
            for NCGI in self.CorrelateMatrixByAppearance[CGI]:
                if NCGI in self.matchedMR and CGI in self.matchedMR[NCGI]:
                    #服务小区在邻小区出现的次数占邻小区总测量报告次数x%以上，且服务小区在邻小区中的电平大于-95dB

                    #if self.matchedMR[NCGI][CGI]['S361']*600 /self.ServCellMR[NCGI] > x and self.matchedMR[NCGI][CGI]['S360']/self.matchedMR[NCGI][CGI]['S361'] >= 15:
                    try:
                        if self.matchedMR[NCGI][CGI]['S361']*600 /sum([self.matchedMR[NCGI][NNCGI]['S361'] for NNCGI in self.matchedMR[NCGI] if NNCGI in self.CGI2GEO and NCGI in self.CGI2GEO and self.CGI2GEO[NNCGI] != self.CGI2GEO[NCGI] or NNCGI not in self.CGI2GEO or NCGI not in self.CGI2GEO]) > x and self.matchedMR[NCGI][CGI]['S360']/self.matchedMR[NCGI][CGI]['S361'] >= 15:
                            try:
                                if self.CGI2GEO[CGI] !=self.CGI2GEO[NCGI]:
                                    #不计入共站址
                                    self.CGICoverageBoundary[CGI].add(NCGI)
                            except KeyError:
                                self.CGICoverageBoundary[CGI].add(NCGI)
                                if CGI not in self.CGI2GEO:
                                    warn = 'CGI {} do not have geoinfo'.format(CGI)
                                    if warn not in self.EscapedWarn:
                                        self.logger.warn('CGI {} do not have geoinfo'.format(CGI))
                                        self.EscapedWarn.append(warn)
                                if NCGI not in self.CGI2GEO:
                                    warn ='Measured NCGI {} do not have geoinfo'.format(NCGI)
                                    if warn not in self.EscapedWarn:
                                        self.logger.warn(warn)
                                        self.EscapedWarn.append(warn)
                    except ZeroDivisionError:
                        self.logger.warn('CGI {} may not have MR report,zero divid encountered.'.format(NCGI))

            relatedCoords = [ self.GEOSets[self.CGI2GEO[SCGI]]['coords'] for SCGI in self.CGICoverageBoundary[CGI] if SCGI in self.CGI2GEO ]

            if relatedCoords:
                try:
                    if HalfCoverage:
                        HalfCoords = []
                        for coords in relatedCoords:
                            HalfCoords.append(( (coords[0]+self.GEOSets[self.CGI2GEO[CGI]]['coords'][0])/2,(coords[1]+self.GEOSets[self.CGI2GEO[CGI]]['coords'][1])/2 ) )
                        convex_hull = shapely.geometry.MultiPoint(HalfCoords+[self.GEOSets[self.CGI2GEO[CGI]]['coords'],]).convex_hull
                    else:
                        convex_hull = shapely.geometry.MultiPoint(relatedCoords+[self.GEOSets[self.CGI2GEO[CGI]]['coords'],]).convex_hull
                except KeyError:
                    #对于没有经纬度的CGI，其覆盖边界不再加入CGI本站的经纬度
                    convex_hull = shapely.geometry.MultiPoint(relatedCoords).convex_hull

                if type(convex_hull) is shapely.geometry.Polygon:
                    #convex_hull_coords = convex_hull.exterior.coords
                    self.CGICoverageBoundary[CGI] = convex_hull
                elif type(convex_hull) is type(shapely.coords.CoordinateSequence) or type(convex_hull) is shapely.geometry.LineString:
                    self.UpdateCoverageByLoosedCriterion(CGI,HalfCoverage)
                else:
                    self.logger.info('CGI {} coverage is not polygon,but {}'.format(CGI,convex_hull))
                    del self.CGICoverageBoundary[CGI]
            else:
            #            if self.CGICoverageBoundary[CGI] :
            #                print('CGI {} heard from neis but nei do not have coords info,so coverage predication failed '.format(CGI))
            #                del self.CGICoverageBoundary[CGI]
            #            else:
            #                #TODO 需要对这类小区用泰森多边形来补充边界
            #                print('CGI {} is un-heardable from neis, coverage predication failed '.format(CGI))
            #                del self.CGICoverageBoundary[CGI]
                self.UpdateCoverageByLoosedCriterion(CGI,HalfCoverage)

            CGICount+=1
            if CGICount*100/len(self.GCELL) % 10 == 0 and (CGICount-1)*100/len(self.GCELL) % 10 != 0:
                self.logger.info('Calc cell coverage {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.GCELL)))

        #print('{} done.'.format('CalcCellCoverageBoundary'))
        self.logger.info('{} done.'.format('CalcCellCoverageBoundary'))


    def OutputCellCoverage(self,filePath,zone=50):
        #TODO FIX Projection system to metrc
        Proj = pyproj.Proj(proj='utm',zone=zone,ellps='WGS84')
        if filePath:
            self.logger.info('Writing cell  coverage map...')
            self.logger.info('{} started....'.format('OutputCellCoverage'))
            with open('{}.coverage.mid'.format(filePath),'wb') as midfp:
                mid = csv.writer(midfp)
                with open('{}.coverage.mif'.format(filePath),'wb') as miffp:
                    miffp.write(
                        '''Version   450
                        Charset "WindowsSimpChinese"
                        Delimiter ","
                        CoordSys Earth Projection 1, 0
                        ''')
                    header = ['CGI','HEX']
                    miffp.write('Columns %s\r\n' % len(header))
                    for col in header:
                        if col in []:
                            miffp.write('  %s Float\r\n' % col)
                        elif col in ['BCCH',]:
                            miffp.write('  %s Integer\r\n' % col)
                        elif col in ['CGI','HEX','BAND',]:
                            miffp.write('  %s Char(50)\r\n' % col)
                        else:
                            miffp.write('  %s Char(254)\r\n' % col)
                    miffp.write('Data\r\n\r\n')

                    for CGI in self.CGICoverageBoundary:
                        miffp.write('Region  1\r\n')
                        miffp.write('  {}\r\n'.format(len (self.CGICoverageBoundary[CGI].exterior.coords)))

                        for point in self.CGICoverageBoundary[CGI].exterior.coords:
                            x,y = Proj(point[0],point[1],inverse=True)
                            miffp.write('{} {}\r\n'.format(x,y))

                        miffp.write('    Pen (1,1,255)\r\n')
                        miffp.write('    Brush (1,255,255)\r\n')
                        endPoint = self.CGICoverageBoundary[CGI].centroid
                        x,y = Proj(endPoint.x,endPoint.y,inverse=True)
                        miffp.write('    Center {} {}\r\n'.format(x,y))
                        HEXCGI = '{:X}{:X}'.format(int(CGI.split('-')[2]),int(CGI.split('-')[3]))
                        mid.writerow([CGI,HEXCGI])
            self.logger.info('Writing cell coverage map done...')
            self.logger.info('{} done....'.format('OutputCellCoverage'))


    def UpdateCoverageByLoosedCriterion(self,CGI,HalfCoverage = True):
    #对不满足n个点多边形进行降低边界条件处理
        self.logger.info('CGI {} coverage is coordinateSequence, try loosen criterion '.format(CGI))
        self.CGICoverageBoundary[CGI] = []
        for NCGI in self.CorrelateMatrixByAppearance[CGI]:
            try:
                if NCGI in self.matchedMR and CGI in self.matchedMR[NCGI] and self.matchedMR[NCGI][CGI]['S360']/self.matchedMR[NCGI][CGI]['S361'] >= 15:
                    try:
                        if self.CGI2GEO[CGI] !=self.CGI2GEO[NCGI] and NCGI not in self.CGICoverageBoundary[CGI]:
                            #不计入共站址
                            self.CGICoverageBoundary[CGI].append([NCGI,self.matchedMR[NCGI][CGI]])
                    except KeyError:
                        pass
            except ZeroDivisionError:
                self.logger.warn('CGI {} Nei {} do not have mr report.'.format(NCGI,CGI))

        relatedCoords = []
        #按照出现的次数排序
        self.CGICoverageBoundary[CGI].sort(key = lambda x:x[1],reverse = True)
        for SCGI in self.CGICoverageBoundary[CGI] :
            if SCGI[0] in self.CGI2GEO and self.GEOSets[self.CGI2GEO[SCGI[0]]]['coords'] not in relatedCoords and len(relatedCoords) < 4:
                relatedCoords.append(self.GEOSets[self.CGI2GEO[SCGI[0]]]['coords'])
        if relatedCoords:
            try:
                if HalfCoverage:
                    HalfCoords = []
                    for coords in relatedCoords:
                        HalfCoords.append(( (coords[0]+self.GEOSets[self.CGI2GEO[CGI]]['coords'][0])/2,(coords[1]+self.GEOSets[self.CGI2GEO[CGI]]['coords'][1])/2 ) )
                    convex_hull = shapely.geometry.MultiPoint(HalfCoords+[self.GEOSets[self.CGI2GEO[CGI]]['coords'],]).convex_hull
                else:
                    convex_hull = shapely.geometry.MultiPoint(relatedCoords+[self.GEOSets[self.CGI2GEO[CGI]]['coords'],]).convex_hull
            except KeyError:
                #对于没有经纬度的CGI，其覆盖边界不再加入CGI本站的经纬度
                convex_hull = shapely.geometry.MultiPoint(relatedCoords).convex_hull
            if type(convex_hull) is shapely.geometry.Polygon:
                #convex_hull_coords = convex_hull.exterior.coords
                self.CGICoverageBoundary[CGI] = convex_hull
            else:
                self.logger.warn('CGI {} Even if loosen the criterion, qualified coverage polygon still can not be generated'.format(CGI))
                self.UpdateCoverageByTa0(CGI)
        else:
            self.logger.warn('CGI {} Even if loosen the criterion, qualified coverage polygon still can not be generated'.format(CGI))
            self.UpdateCoverageByTa0(CGI)

    def UpdateCoverageByTa0(self,CGI):
        #生成基于CGI 0 TA 的四边形
        if CGI in self.CGI2GEO:
            self.logger.info('CGI {},use default 0 TA coverage'.format(CGI))
            if self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] == 'indoor':
                relatedCoords = [
                    (self.GEOSets[self.CGI2GEOI2GEO[CGI]]['coords'][0]-225,self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]),
                    (self.GEOSets[self.CGI2GEO[CGI]]['coords'][0],self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]+255),
                    (self.GEOSets[self.CGI2GEO[CGI]]['coords'][0]+255,self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]),
                    (self.GEOSets[self.CGI2GEO[CGI]]['coords'][0],self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]-255)
                ]
            else:
                relatedCoords = [
                    (self.GEOSets[self.CGI2GEO[CGI]]['coords'][0]-550,self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]),
                    (self.GEOSets[self.CGI2GEO[CGI]]['coords'][0],self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]+550),
                    (self.GEOSets[self.CGI2GEO[CGI]]['coords'][0]+550,self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]),
                    (self.GEOSets[self.CGI2GEO[CGI]]['coords'][0],self.GEOSets[self.CGI2GEO[CGI]]['coords'][1]-550)
                ]
            convex_hull = shapely.geometry.MultiPoint(relatedCoords).convex_hull
            if type(convex_hull) is shapely.geometry.Polygon:
                #convex_hull_coords = convex_hull.exterior.coords
                self.CGICoverageBoundary[CGI] = convex_hull
                return True
            else:
                self.logger.warn('FAILED to generate {} default 0 TA coverage,unknown error.'.format(CGI))
                if CGI in self.CGICoverageBoundary:
                    del self.CGICoverageBoundary[CGI]
        else:
            self.logger.warn('FAILED to generate {} default 0 TA coverage,missing coordinates.'.format(CGI))
            if CGI in self.CGICoverageBoundary:
                del self.CGICoverageBoundary[CGI]

        return False

    def GridCoverageDetect(self,zone = 50):
        #TODO FIX ME Project system
        Proj = pyproj.Proj(proj='utm',zone = zone,ellps='WGS84')
        self.logger.info('{} started....'.format('GridCoverageDetect'))
        self.logger.info('Grid sample calc start....')

        for x in xrange(self.Xmin,self.Xmax+self.GRID_SIZE,self.GRID_SIZE):
            for y in xrange(self.Ymin,self.Ymax+self.GRID_SIZE,self.GRID_SIZE):
                self.GridMatrix[(x,y)] = {}
                #self.GridMatrix[(x,y)]['lonlat'] = Proj(x,y,inverse=True)

        CGICount = 0

        for CGI in self.CGICoverageBoundary:
            self.CGICoverageGridsByTA[CGI] = {}
            if self.CGICoverageBoundary[CGI]:
                cXmax = int(self.CGICoverageBoundary[CGI].bounds[2])
                cXmin = int(self.CGICoverageBoundary[CGI].bounds[0])
                cYmax = int(self.CGICoverageBoundary[CGI].bounds[3])
                cYmin = int(self.CGICoverageBoundary[CGI].bounds[1])

                startX = self.GRID_SIZE - abs(self.Xmin - cXmin) % self.GRID_SIZE + cXmin
                startY = self.GRID_SIZE - abs(self.Ymin - cYmin) % self.GRID_SIZE + cYmin
                if (startX,startY) not in self.GridMatrix:
                    self.logger.warn('start point guess miss,cxmin {},cymin {},xmin {},ymin {},guessed {},{} '.format(cXmin,cYmin,self.Xmin,self.Ymin,startX,startY))
                    continue
                else:
                    for x in xrange(startX,cXmax,self.GRID_SIZE):
                        for y in xrange(startY,cYmax,self.GRID_SIZE):
                            grid = shapely.geometry.Point(x,y)
                            #if self.CGICoverageBoundary[CGI].contains(grid):
                            gridBoundary = shapely.geometry.Polygon([(x-self.GRID_SIZE/2,y+self.GRID_SIZE/2),(x+self.GRID_SIZE/2,y+self.GRID_SIZE/2),(x+self.GRID_SIZE/2,y-self.GRID_SIZE/2),(x-self.GRID_SIZE/2,y-self.GRID_SIZE/2)])
                            if self.CGICoverageBoundary[CGI].intersects(gridBoundary):
                                try:
                                    self.GridMatrix[(x,y)][CGI] = {'TA': int(grid.distance(shapely.geometry.Point(self.GEOSets[self.CGI2GEO[CGI]]['coords'])) ) // 550 }
                                except KeyError:
                                    if (x,y) not in self.GridMatrix:
                                        #Caused by TA 0 coverage, I guess
                                        self.GridMatrix[(x,y)] = {}
                                        self.GridMatrix[(x,y)][CGI] = {'TA': int(grid.distance(shapely.geometry.Point(self.GEOSets[self.CGI2GEO[CGI]]['coords'])) ) // 550 }
                                    elif CGI not in self.CGI2GEO:
                                        self.GridMatrix[(x,y)][CGI] = {'TA': 0 }
                                        warn = 'CGI {} do not have coordinates info,use default TA 0'.format(CGI)
                                        if warn not in self.EscapedWarn:
                                            self.logger.warn(warn)
                                            self.EscapedWarn.append(warn)
                                finally:
                                    try:
                                        self.CGICoverageGridsByTA[CGI][self.GridMatrix[(x,y)][CGI]['TA']] += 1
                                    except KeyError:
                                        self.CGICoverageGridsByTA[CGI][self.GridMatrix[(x,y)][CGI]['TA']] = 1
            CGICount+=1
            if CGICount*100/len(self.CGICoverageBoundary) % 10 == 0 and (CGICount-1)*100/len(self.CGICoverageBoundary) % 10 != 0:
                self.logger.info('Calc grid sample {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.CGICoverageBoundary)))
            #print(self.GridMatrix)
        self.logger.info('Grid sample calc done.')
        self.logger.info('{} done....'.format('GridCoverageDetect'))


    def TA_ERL_Parser(self,filePath,recordHours):
        cellCount = 0
        self.logger.info('{} started....'.format('TA_ERL_Parser'))
        if filePath:
            path,mfile = os.path.split(filePath.lower())
            main,ext = os.path.splitext(mfile)

            for cfile in os.listdir(path):
                if not fnmatch.fnmatch(cfile.lower(),'{}*{}'.format(main,ext)):
                    continue
                self.logger.info('Found match file {}'.format(cfile))
                pth = os.path.join(path,cfile)
                with open(pth,'rb') as fp:
                    self.logger.info('file opened.')
                    reader = csv.reader(fp)
                    self.logger.info('matching header')
                    idx_GCELL_NCELL = None
                    idx_LAC = None
                    idx_CI = None
                    idx_TCH = None
                    idx_PDCH = None
                    idx_NCC = None
                    idx_uplinkBytes = None
                    idx_downlinkBytes = None
                    idx_TAx = {}
                    for row in reader:
                        if 'GBSC' in row :
                            #For PRS report
                            idx_LAC = row.index('小区LAC')
                            idx_CI = row.index('小区CI')
                            for i in range(len(row)):
                                if 'K3014' in row[i]:
                                    idx_TCH = i
                                elif 'AR9311' in row[i]:
                                    idx_PDCH = i
                                elif 'L9506' in row[i]:
                                    idx_downlinkBytes = i
                                elif 'L9403' in row[i]:
                                    idx_uplinkBytes = i
                                elif 'S44' in row[i]:
                                    idx_TAx[int(row[i][3:row[i].index('A')])] = i
                            self.logger.info('Header successfully matched.')
                        elif 'GCELL' in row :
                            #For M2000 report
                            #TODO M2000部分需要补充
                            continue
                        elif idx_LAC or idx_GCELL_NCELL:
                            #actual data part
                            try:
                                if idx_LAC:
                                    CGI = '{}-{}-{}-{}'.format('460','00',row[idx_LAC],row[idx_CI])
                                else:
                                    #TODO M2000部分需要补充
                                    continue

                                try:
                                    if idx_TCH and idx_PDCH:
                                        try:
                                            self.CGITraffic_TA[CGI]
                                        except KeyError:
                                            self.CGITraffic_TA[CGI] = {}
                                            self.CGITraffic_TA[CGI]['K3014'] = 0
                                            self.CGITraffic_TA[CGI]['AR9311'] = 0
                                            self.CGITraffic_TA[CGI]['L9506'] = 0
                                            self.CGITraffic_TA[CGI]['L9403'] = 0
                                        finally:
                                            self.CGITraffic_TA[CGI]['K3014'] += float(row[idx_TCH])/recordHours
                                            self.CGITraffic_TA[CGI]['AR9311'] += float(row[idx_PDCH])/recordHours
                                            self.CGITraffic_TA[CGI]['L9506'] += float(row[idx_downlinkBytes])/recordHours
                                            self.CGITraffic_TA[CGI]['L9403'] += float(row[idx_uplinkBytes])/recordHours
                                except :
                                    self.logger.warn('CGI {} parser TCH/PDCH LLC failed'.format(CGI))
                                    raise

                                try:
                                    if idx_TAx:
                                        try:
                                            self.CGITraffic_TA[CGI]['MRsByTA']
                                        except KeyError:
                                            if CGI not in self.CGITraffic_TA:
                                                self.CGITraffic_TA[CGI] = {}
                                            self.CGITraffic_TA[CGI]['MRsByTA'] = {}
                                            for i in range(0,64):
                                                self.CGITraffic_TA[CGI]['MRsByTA'][i] = 0
                                        finally:
                                            for ta in idx_TAx:
                                                self.CGITraffic_TA[CGI]['MRsByTA'][ta] += int(row[idx_TAx[ta]])
                                except :
                                    self.logger.warn('CGI {} parser TA failed'.format(CGI))
                                    raise
                            except IndexError:
                                #For PRS report end line
                                continue
        self.logger.info('TA parser done...')
        self.logger.info('{} done..'.format('TA_ERL_Parser'))

    def CalcCGI_TaGridsWeight(self,CGI):
        '''
        计算CGI在不同TA/GRID下话务量的权重
        '''
        if CGI in self.CGITraffic_TA:
            Ta_Count = self.CGITraffic_TA[CGI]['MRsByTA'].items()
            TaGrids = self.CGICoverageGridsByTA[CGI].keys()
            TAGridMRs = dict([(ta,0) for ta in TaGrids])
            for pair in Ta_Count:
                if pair[0] in TaGrids:
                    TAGridMRs[pair[0]] += pair[1]
                else:
                    for ta in range(pair[0],-1,-1):
                        if ta in TaGrids:
                            TAGridMRs[ta] += pair[1]
                            break
                    else:
                        for ta in range(pair[0],64,1):
                            if ta in TaGrids:
                                TAGridMRs[ta] += pair[1]
                                break
                        else:
                            self.logger.warn('CGI {} have TA = {} MR records,but missing according grid.'.format(CGI,pair[0]))

            self.CGIErlWeightsByTaGrids[CGI] = {}
            for ta in TAGridMRs:
                try:
                    self.CGIErlWeightsByTaGrids[CGI][ta] = 1.0*TAGridMRs[ta]/sum(TAGridMRs.values())/self.CGICoverageGridsByTA[CGI][ta]
                except ZeroDivisionError:
                    self.CGIErlWeightsByTaGrids[CGI][ta] = 0
        else:
            self.logger.info('CGI {} missing TA distribution datum,using averaged distribution by total grids.'.format(CGI))
            for ta in self.CGICoverageGridsByTA[CGI]:
                self.CGIErlWeightsByTaGrids[CGI][ta] = 1.0/sum(self.CGICoverageGridsByTA[CGI].values())

    def CalcDelauNeis(self):
        #仅对宏站进行邻区关系计算
        self.logger.info('Start calculate delny neis')
        #Do some clean stuff
        for coord in self.GEOSets:
            self.GEOSets[coord]['neis'] = []
            self.GEOSets[coord]['triangle']=set()

        t = delaunay.Triangulation( [ coord for coord in self.GEOSets if 'macro' in list(self.GEOSets[coord]['CoverageType'])  ])
        nei_sets = t.get_neighbours()
        for coords in nei_sets:
            self.GEOSets[coords]['neis'] = nei_sets[coords]
        for tri in t.get_elements():
            for coords in tri:
                try:
                    self.GEOSets[coords]['triangle'].add(frozenset(tri))
                except KeyError:
                    self.GEOSets[coords]['triangle']=set()
                    self.GEOSets[coords]['triangle'].add(frozenset(tri))
        self.logger.info('Delny neis calculation complete.')

    def CalcGridSiteDist(self):
        self.logger.info('Start calc grid site distance...')
        processed_tri = []
        for coord in self.GEOSets:
            for tri in  self.GEOSets[coord].get('triangle',[]):
                if tri in processed_tri:
                    continue
                else:
                    processed_tri.append(tri)
                    tri = list(tri)
                    dist1 = shapely.geometry.Point(self.GEOSets[tri[0]]['coords']).distance(shapely.geometry.Point(self.GEOSets[tri[1]]['coords']))
                    dist2 = shapely.geometry.Point(self.GEOSets[tri[2]]['coords']).distance(shapely.geometry.Point(self.GEOSets[tri[1]]['coords']))
                    dist3 = shapely.geometry.Point(self.GEOSets[tri[0]]['coords']).distance(shapely.geometry.Point(self.GEOSets[tri[2]]['coords']))
                    avg_dist = sum([dist1,dist2,dist3])/3

                    poly = shapely.geometry.Polygon([ self.GEOSets[coord]['coords'] for coord in tri ])
                    cXmax = int(poly.bounds[2])
                    cXmin = int(poly.bounds[0])
                    cYmax = int(poly.bounds[3])
                    cYmin = int(poly.bounds[1])

                    startX = self.GRID_SIZE - abs(self.Xmin - cXmin) % self.GRID_SIZE + cXmin
                    startY = self.GRID_SIZE - abs(self.Ymin - cYmin) % self.GRID_SIZE + cYmin
                    if (startX,startY) not in self.GridMatrix:
                        self.logger.warn('start point guess miss,cxmin {},cymin {},xmin {},ymin {},guessed {},{} '.format(cXmin,cYmin,self.Xmin,self.Ymin,startX,startY))
                        continue
                    else:
                        for x in xrange(startX,cXmax,self.GRID_SIZE):
                            for y in xrange(startY,cYmax,self.GRID_SIZE):
                                if (x,y) in self.GridMatrix and poly.contains(shapely.geometry.Point(x,y)):
                                    self.GridSiteDist[(x,y)] = avg_dist
        self.logger.info('Calc grid site distance done.')

    def getCellDistGradeByDelny(self,CGI,base=500):
        if CGI in self.CGI2GEO and self.GEOSets[ self.CGI2GEO[CGI] ].get('neis'):
            cell = shapely.geometry.Point(self.GEOSets[self.CGI2GEO[CGI]]['coords'])
            try:
                return int(sum([ shapely.geometry.Point(self.GEOSets[coord]['coords']).distance(cell) for coord in self.GEOSets[ self.CGI2GEO[CGI] ]['neis'] ])/len(self.GEOSets[ self.CGI2GEO[CGI] ]['neis'])/500) + 1
            except ZeroDivisionError:
                return 1
        else:
            return 1

    def _CalcCellDistByDelnyMR(self,CGI):
        if CGI in self.CGI2GEO and CGI in self.GCELL :
            self.GCELL[CGI]['ncell'] = set()
            self.GCELL[CGI]['ncell_dist_avg'] = None
            cell = shapely.geometry.Point(self.GEOSets[self.CGI2GEO[CGI]]['coords'])
            if  self.GEOSets[ self.CGI2GEO[CGI] ].get('neis'):
                for NCGI in self.CorrelateMatrixByAppearance:
                    if NCGI in self.CGI2GEO and  self.CGI2GEO[NCGI]  in self.GEOSets[ self.CGI2GEO[CGI] ]['neis']:
                        self.GCELL[CGI]['ncell'].add(NCGI)
                try:
                    self.GCELL[CGI]['ncell_dist_avg'] = sum([ shapely.geometry.Point(self.GEOSets[self.CGI2GEO[NCGI]]['coords']).distance(cell) for NCGI in self.GCELL[CGI]['ncell'] ])/len(self.GCELL[CGI]['ncell'])
                except ZeroDivisionError:
                    pass
        elif CGI not in self.GCELL:
            warn ='CGI {} is not in MML Config,nei dist calc will be escaped'.format(CGI)
            if warn not in self.EscapedWarn:
                self.logger.warn(warn)
                self.EscapedWarn.append(warn)
        elif not self.GCELL[CGI].get('EXTCELL'):
            warn ='CGI {} do not have coords ,nei dist calc will be escaped'.format(CGI)
            if warn not in self.EscapedWarn:
                self.logger.warn(warn)
                self.EscapedWarn.append(warn)

    def CalcCellDist(self):
        for CGI in self.GCELL:
            self._CalcCellDistByDelnyMR(CGI)

    def CalcGridErlDensity(self,ZeraSampleAllowed = False):
        #计算网格话务量,当栅格-CGI对应TA中无MR时，在该栅格中
        self.logger.info('Start calc Grid ERL density...')
        for grid in self.GridMatrix:
            for CGI in self.GridMatrix[grid].keys():
                if CGI in self.CGITraffic_TA:
                    if CGI not in self.CGIErlWeightsByTaGrids:
                        self.CalcCGI_TaGridsWeight(CGI)
                    self.GridMatrix[grid][CGI]['ERL'] = self.CGIErlWeightsByTaGrids[CGI][ self.GridMatrix[grid][CGI]['TA'] ] * ( self.CGITraffic_TA[CGI]['K3014'] + self.CGITraffic_TA[CGI]['AR9311'])
                    if not ZeraSampleAllowed and self.CGIErlWeightsByTaGrids[CGI][ self.GridMatrix[grid][CGI]['TA'] ] == 0:
                        if self.CGICoverageGridsByTA[CGI][self.GridMatrix[grid][CGI]['TA']] > 0:
                            self.CGICoverageGridsByTA[CGI][self.GridMatrix[grid][CGI]['TA']] -= 1
                        else:
                            del self.CGICoverageGridsByTA[CGI][self.GridMatrix[grid][CGI]['TA']]
                        del self.GridMatrix[grid][CGI]
                else:
                    self.GridMatrix[grid][CGI]['ERL'] = 0

                #                if ZeraSampleAllowed:
                #                    self.GridMatrix[grid][CGI]['ERL'] = 0
                #                else:
                #                    del self.GridMatrix[grid][CGI]
                #                    del self.CGICoverageGridsByTA[CGI][self.GridMatrix[grid][CGI]['TA']]

        self.logger.info('Grid ERL density done.')

    def RestoreCGIERL(self,filePath):
        if filePath:
            self.logger.info('Write reverse ERL check file..')
            with open('{}.revERL.csv'.format(filePath),'wb') as fp:
                writer = csv.writer(fp)
                ERL = {}
                for grid in self.GridMatrix:
                    for CGI in self.GridMatrix[grid]:
                        try:
                            ERL[CGI]+= self.GridMatrix[grid][CGI]['ERL']
                        except KeyError:
                            ERL[CGI] = self.GridMatrix[grid][CGI]['ERL']
                writer.writerow(['CGI','ERL'])
                for CGI in ERL:
                    writer.writerow([CGI,ERL[CGI]*6])
            self.logger.info('Done.')

    def MRParser(self,mrFiles):
        #global RAW_MR
        #print('{} started....'.format('MRParser'))
        self.logger.info('{} started....'.format('MRParser'))
        mr_count = 0
        if not mrFiles:
            return False
        for cfile in mrFiles:
            self.logger.info(u'Found match file {}'.format(cfile))
            try:
                with open(cfile,'rb') as fp:
                    #print('file opened.')
                    self.logger.info('file opened.')
                    reader = csv.reader(fp)
                    #print('matching header')
                    self.logger.info('matching header')
                    idx_GCELL_NCELL = None
                    idx_LAC = None
                    idx_CI = None
                    idx_BCCH = None
                    idx_BCC = None
                    idx_NCC = None
                    idx_S361 = None
                    idx_S369 = None
                    idx_S360 = None
                    for row in reader:
                        try:
                            if 'GBSC' in row :
                                #For PRS report
                                idx_LAC = row.index('小区LAC')
                                idx_CI = row.index('小区CI')
                                idx_BCCH = row.index('BCCH')
                                idx_NCC = row.index('小区网络色码')
                                idx_BCC = row.index('小区基站色码')
                                for i in range(len(row)):
                                    if 'S361:' in row[i]:
                                        idx_S361 = i
                                    elif 'S369:' in row[i]:
                                        idx_S369 = i
                                    elif 'S360:' in row[i]:
                                        idx_S360 = i
                                if not idx_S361 or not idx_S369:
                                    self.logger.error('Missing Counter S361 or S369 in MR file {}.'.format(cfile))
                                    break
                                    #print('Header successfully matched.')
                                self.logger.info('Header successfully matched.PRS Report')
                            elif 'GCELL_NCELL' in row :
                                #For M2000 report
                                idx_GCELL_NCELL = row.index('GCELL_NCELL')
                                idx_BCCH = row.index('BCCH')
                                idx_NCC = row.index('NCC')
                                idx_BCC = row.index('BCC,NCC')
                                for i in range(len(row)):
                                    if 'S361:' in row[i]:
                                        idx_S361 = i
                                    elif 'S369:' in row[i]:
                                        idx_S369 = i
                                    elif 'S360:' in row[i]:
                                        idx_S360 = i
                                if not idx_S361 or not idx_S369:
                                    self.logger.error('Missing Counter S361 or S369 in MR file {}.'.format(cfile))
                                    break
                                self.logger.info('Header successfully matched,M2000 Report.')
                            elif idx_LAC or idx_GCELL_NCELL:
                                #raw_name = row[idx_GCELL_NCELL]
                                #raw_CGI = raw_name.split(',')[-1].split('=')[-1]
                                if idx_LAC:
                                    CGI = '{}-{}-{}-{}'.format('460','00',row[idx_LAC],row[idx_CI])
                                else:
                                    raw_name = row[idx_GCELL_NCELL]
                                    raw_CGI = raw_name.split(',')[-1].split('=')[-1]
                                    CGI = '{}-{}-{}-{}'.format(raw_CGI[0:3],raw_CGI[3:5],int(raw_CGI[5:9],16),int(raw_CGI[9:],16))

                                mr_count+=1

                                BCCH_BSIC = '{}#{}#{}'.format(row[idx_BCCH],row[idx_NCC],row[idx_BCC])

                                try:
                                    self.ServCellMR[CGI] += int(float(row[idx_S361]))
                                except KeyError:
                                    self.ServCellMR[CGI] = int(float(row[idx_S361]))
                                except TypeError:
                                    continue

                                try:
                                    self.RAW_MR[CGI][BCCH_BSIC]['S361'] += int(float(row[idx_S361]))
                                    self.RAW_MR[CGI][BCCH_BSIC]['S369'] += int(float(row[idx_S369]))
                                    self.RAW_MR[CGI][BCCH_BSIC]['S360'] += int(float(row[idx_S360]))
                                except:
                                    try:
                                        self.RAW_MR[CGI]
                                    except:
                                        self.RAW_MR[CGI] = {}
                                    finally:
                                        self.RAW_MR[CGI][BCCH_BSIC] = {'S361':0,'S369':0,'S360':0}

                                    self.RAW_MR[CGI][BCCH_BSIC]['S361'] += int(float(row[idx_S361]))
                                    self.RAW_MR[CGI][BCCH_BSIC]['S369'] += int(float(row[idx_S369]))
                                    self.RAW_MR[CGI][BCCH_BSIC]['S360'] += int(float(row[idx_S360]))
                                    try:
                                        self.RAW_MR[CGI][BCCH_BSIC]['S360/S361'] = self.RAW_MR[CGI][BCCH_BSIC]['S360']/self.RAW_MR[CGI][BCCH_BSIC]['S361']
                                    except :
                                        self.RAW_MR[CGI][BCCH_BSIC]['S360/S361'] = 0
                            else:
                                self.logger.warn('Escaping:{}'.format(row))
                        except IndexError:
                            continue
            except IOError:
                self.logger.error('Failed to open {}'.format(cfile))
        self.logger.info('self.RAW_MR len {}'.format(len(self.RAW_MR)))
        self.logger.info('{} mr records readed.'.format(mr_count))
        return True




    def MR_Nei_Match(self):
        self.logger.info('{} started....'.format('MR_Nei_Match'))

        if not self.RAW_MR:
            self.logger.info('No parsered MRs detected.')
            return False

        CGICount = 0
        maxCount = len(self.RAW_MR)

        for CGI in self.RAW_MR:
        #        self.logger.info('DEBUG look CGI {}'.format(CGI))
        #        self.logger.info('PRINT self.RAW_MR[CGI]:{}'.format(self.RAW_MR[CGI]))
            for BB in self.RAW_MR[CGI].keys():
                NeiCandidate = set(self.G2GNCELL.get(CGI,[]))
                BBCandidate = self.BCCH_BSIC_IDX.get(BB,set([]))

                joinResult = NeiCandidate.intersection(BBCandidate)
                if joinResult:
                    #如果BB对应已知的CGI，且在CGI的G2GNCELL表中有该小区，则却认为已定义邻区
                    if len(joinResult) > 1:
                        confusedNei = []
                        while joinResult:
                            confusedNei.append(joinResult.pop())
                            #只返回多重匹配中的第一个，可能会导致不可预期的误差
                        #self.RAW_MR[CGI][BB]['NCGI'] = confusedNei
                        #self.RAW_MR[CGI][BB]['mapCount'] = len(confusedNei)
                        self.RAW_MR[CGI][BB]['NCGI'] = confusedNei[0]
                        self.RAW_MR[CGI][BB]['Defined'] = True
                        self.RAW_MR[CGI][BB]['mapCount'] = 1
                        #只返回多重匹配中的第一个，可能会导致不可预期的误差
                        warn ='Unexpect BCCH-BSIC {} multi-match with Nei {} for CGI {},only using {}'.format(BB,confusedNei,CGI,self.RAW_MR[CGI][BB]['NCGI'])
                        if warn not in self.EscapedWarn:
                            #print(warn)
                            self.logger.warn(warn)
                            self.EscapedWarn.append(warn)
                    else:
                        self.RAW_MR[CGI][BB]['NCGI'] = joinResult.pop()
                        self.RAW_MR[CGI][BB]['Defined'] = True
                        self.RAW_MR[CGI][BB]['mapCount'] = 1
                else:
                    #None Nei Resolve will do                    
                    self.RAW_MR[CGI][BB]['NCGI'] = self.ResolveUndefinedBB(CGI,BB,[])
                    self.RAW_MR[CGI][BB]['Defined'] = False
                    if type(str) == type(self.RAW_MR[CGI][BB]['NCGI']):
                        self.RAW_MR[CGI][BB]['mapCount'] = 1
                    elif type(list) == type(self.RAW_MR[CGI][BB]['NCGI']):
                        self.RAW_MR[CGI][BB]['mapCount'] = len(self.RAW_MR[CGI][BB]['NCGI'])
                    else:
                        self.RAW_MR[CGI][BB]['mapCount'] = 0                    

            CGICount+=1
            if CGICount*100/maxCount % 10 == 0 and (CGICount-1)*100/maxCount % 10 != 0:
                self.logger.info('Matching processed cell {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.RAW_MR)))
                self.progressQueue.put(CGICount*100/maxCount)

        self.isMrMatched = True
        self.logger.info('Nei Match done.')

    def blackMrFiltering(self,ratio = 0.05,rxlev = 30):
        self.logger.info('Black MR filter started...')
        'CREATE TABLE IF NOT EXISTS blackMR (scgi TEXT ,blackID TEXT,blackMrs INTEGER,serverMRs REAL)'
        dbConn = self.parent._acquireDBConn()
        if not dbConn:
            self.logger.error('Database connection not created!')
            return False
        try:
            #dbConn.execute('DROP TABLE IF EXISTS cellNetworkInfo')
            dbConn.execute('DROP TABLE IF EXISTS blackMR')
            dbConn.execute(self.createBlackMrTableSql)
            dbConn.commit()
            cursor = dbConn.cursor()
            sql = 'INSERT INTO blackMR (scgi,blackID,blackMrs,serverMRs) VALUES (?,?,?,?)'
            count = 0
            maxCount = len(self.RAW_MR)
            for CGI in self.RAW_MR:
                count += 1
                for BB in self.RAW_MR[CGI]:
                    try:
                        if not self.RAW_MR[CGI][BB]['NCGI']  and self.RAW_MR[CGI][BB]['S361'] *6.0 / self.ServCellMR[CGI] >= ratio and self.RAW_MR[CGI][BB]['S360/S361'] >= rxlev:
                            cursor.execute(sql,(CGI,BB,self.RAW_MR[CGI][BB]['S361'],self.ServCellMR[CGI]/6.0))
                    except ZeroDivisionError:
                        pass
                self.progressQueue.put(count*100/maxCount)
            dbConn.commit()
            dbConn.execute('DELETE FROM projectinfo WHERE attribute = "dataload" AND value = "blackMR"')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","blackMR",datetime("now"))')
            dbConn.commit()
            self.logger.info('Blakc MR filter complete.')
            self.isBlackMrFitered = True
            return True
        except:
            self.logger.exception('Failed to filter black MR,unknown error.')
            return False
        finally:
            self.parent._releaseDBConn()

    def resizeRawMrSpace(self):
        self.logger.info('{} started....'.format('Resize Raw MR space started'))

        if not self.RAW_MR:
            self.logger.info('No parsered MRs detected.')
            return False

        CGICount = 0
        maxCount = len(self.RAW_MR)

        for CGI in self.RAW_MR:
            for BB in self.RAW_MR[CGI].keys():
                if not self.RAW_MR[CGI][BB]['NCGI']:
                    del self.RAW_MR[CGI][BB]

            CGICount+=1
            if CGICount*100/maxCount % 10 == 0 and (CGICount-1)*100/maxCount % 10 != 0:
                self.logger.info('Resizing processed cell {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.RAW_MR)))
                self.progressQueue.put(CGICount*100/maxCount)

        self.logger.info('Resize Raw MR space done.')

    def uninstallRawMr(self):
        self.logger.warn('Uninstalling Raw MR data!')
        self.RAW_MR = {}

    def __decrypted_MR_Nei_Match(self):
        self.logger.info('{} started....'.format('MR_Nei_Match'))

        if not self.RAW_MR:
            self.logger.info('No parsered MRs detected.')
            return False

        CGICount = 0
        maxCount = len(self.RAW_MR)

        for CGI in self.RAW_MR:
        #        self.logger.info('DEBUG look CGI {}'.format(CGI))
        #        self.logger.info('PRINT self.RAW_MR[CGI]:{}'.format(self.RAW_MR[CGI]))
            for BB in self.RAW_MR[CGI]:
                NeiCandidate = set(self.G2GNCELL.get(CGI,[]))
                BBCandidate = self.BCCH_BSIC_IDX.get(BB,set([]))

                joinResult = NeiCandidate.intersection(BBCandidate)
                if joinResult:
                    #如果BB对应已知的CGI，且在CGI的G2GNCELL表中有该小区，则却认为已定义邻区
                    if len(joinResult) > 1:
                        confusedNei = []
                        while joinResult:
                            confusedNei.append(joinResult.pop())
                            #只返回多重匹配中的第一个，可能会导致不可预期的误差
                        #self.RAW_MR[CGI][BB]['NCGI'] = confusedNei
                        #self.RAW_MR[CGI][BB]['mapCount'] = len(confusedNei)
                        self.RAW_MR[CGI][BB]['NCGI'] = confusedNei[0]
                        self.RAW_MR[CGI][BB]['Defined'] = True
                        self.RAW_MR[CGI][BB]['mapCount'] = 1
                        #只返回多重匹配中的第一个，可能会导致不可预期的误差
                        warn ='Unexpect BCCH-BSIC {} multi-match with Nei {} for CGI {},only using {}'.format(BB,confusedNei,CGI,self.RAW_MR[CGI][BB]['NCGI'])
                        if warn not in self.EscapedWarn:
                            #print(warn)
                            self.logger.warn(warn)
                            self.EscapedWarn.append(warn)
                    else:
                        self.RAW_MR[CGI][BB]['NCGI'] = joinResult.pop()
                        self.RAW_MR[CGI][BB]['Defined'] = True
                        self.RAW_MR[CGI][BB]['mapCount'] = 1
                else:
                    #None Nei Resolve will do
                    self.RAW_MR[CGI][BB]['NCGI'] = self.ResolveUndefinedBB(CGI,BB,[])
                    self.RAW_MR[CGI][BB]['Defined'] = False
                    if type(str) == type(self.RAW_MR[CGI][BB]['NCGI']):
                        self.RAW_MR[CGI][BB]['mapCount'] = 1
                    elif type(list) == type(self.RAW_MR[CGI][BB]['NCGI']):
                        self.RAW_MR[CGI][BB]['mapCount'] = len(self.RAW_MR[CGI][BB]['NCGI'])
                    else:
                        self.RAW_MR[CGI][BB]['mapCount'] = 0
                    #                if not self.RAW_MR[CGI][BB]['CGI']:
                    #                    print('Found undefined BCCB-BSIC {} for CGI {}'.format(BB,CGI))
                    #                else:
                    #                    print('Resolve successfull for {}@{}'.format(BB,CGI))
            CGICount+=1
            #if CGICount*100/len(self.RAW_MR) % 10 == 0 and (CGICount-1)*100/len(self.RAW_MR) % 10 != 0:
                #print('Matching processed cell {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.RAW_MR)))
            self.progressQueue.put(CGICount*100/maxCount)
                #self.logger.info('Matching processed cell {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.RAW_MR)))
                #self.RAW_MR.sync()


        self.isMrMatched = True
        self.logger.info('Nei Match done.')

    def ResolveUndefinedBB(self,CGI,BB,escapeNCGI,StackDepth = 0):
        if StackDepth > self.NEI_RESOLVE_MAX_DEPTH:
            return None
        if CGI and BB:
            NeiCandidate = set(self.G2GNCELL.get(CGI,[]))
            BBCandidate = self.BCCH_BSIC_IDX.get(BB,set([]))
            joinResult = NeiCandidate.intersection(BBCandidate)
            if joinResult:
                if len(joinResult) > 1:
                    confusedNei = []
                    while joinResult:
                        confusedNei.append(joinResult.pop())
                        #print('Unexpect BCCH-BSIC {} multi-match with Nei {} for CGI {}'.format(BB,confusedNei,CGI))
                    warn ='Unexpect BCCH-BSIC {} multi-match with Nei {} for CGI {},only {} will be used'.format(BB,confusedNei,CGI,confusedNei[0])
                    if warn not in self.EscapedWarn:
                        #print(warn)
                        self.logger.warn(warn)
                        self.EscapedWarn.append(warn)
                        #return confusedNei
                    return confusedNei[0]
                else:
                    return joinResult.pop()
            else:
                subCandidates = []
                for NCGI in NeiCandidate:
                    if BB in self.RAW_MR.get(NCGI,[]) and self.RAW_MR.get(NCGI,[])[BB]['S361'] > 0 and not NCGI in escapeNCGI:
                        #print('Resolve BB {} for CGI {} at NCGI {} with S361 {}'.format(BB,CGI,NCGI,self.RAW_MR.get(NCGI,[])[BB]['S361']))
                        escapeNCGI.append(NCGI)
                        subNCGI = self.ResolveUndefinedBB(NCGI,BB,escapeNCGI,StackDepth+1)
                        if subNCGI:
                            subCandidates.append([subNCGI,self.RAW_MR[NCGI][BB]['S361']])
                            #Newly added to avoid long finding trip
                        #                        subCandidates.sort(key = lambda x:x[1],reverse = True)
                        #                        if len(subCandidates) >= 3 and subCandidates[0][0] == subCandidates[1][0] and subCandidates[2][0] == subCandidates[1][0]:
                        #                            break
                if subCandidates:
                    subCandidates.sort(key = lambda x:x[1],reverse = True)
                    return subCandidates[0][0]
                else:
                    return None
        else:
            return None

    def reIndexGcellRelation(self,MatrixBandBased = True):
        '''
        MatrixBandBased = True 代表相关矩阵的生成仅限于同频小区
        '''
        #print('{} started....'.format('reIndexGcellRelation'))
        self.logger.info('{} started....'.format('reIndexGcellRelation'))
        for CGI in self.RAW_MR:
            for BB in self.RAW_MR[CGI]:

                if not self.RAW_MR[CGI][BB]['NCGI']:
                    continue
                NCGIS = []
                if self.RAW_MR[CGI][BB]['mapCount'] > 1:
                    NCGIS = self.RAW_MR[CGI][BB]['NCGI']
                elif 1 == self.RAW_MR[CGI][BB]['mapCount']:
                    NCGIS = [self.RAW_MR[CGI][BB]['NCGI'],]
                for NCGI in NCGIS:
                    try:
                        self.matchedMR[CGI][NCGI] = {}
                    except KeyError:
                        self.matchedMR[CGI] = {}
                        self.matchedMR[CGI][NCGI] = {}
                    finally:
                        self.matchedMR[CGI][NCGI]['S361'] = self.RAW_MR[CGI][BB]['S361']
                        self.matchedMR[CGI][NCGI]['S369'] = self.RAW_MR[CGI][BB]['S369']
                        self.matchedMR[CGI][NCGI]['S360'] = self.RAW_MR[CGI][BB]['S360']
                        self.matchedMR[CGI][NCGI]['S360/S361'] = self.RAW_MR[CGI][BB]['S360/S361']

                        self.matchedMR[CGI][NCGI]['COBAND'] = ( int(self.GCELL[NCGI]['BCCH']) in range(512,886)) ==( int(self.GCELL[CGI]['BCCH']) in range(512,886))
                        try:
                            #self.matchedMR[CGI][NCGI]['COR_COE'] = (self.RAW_MR[CGI][BB]['S361'] -  self.RAW_MR[CGI][BB]['S369'])*1.0 /  self.RAW_MR[CGI][BB]['S361']
                            self.matchedMR[CGI][NCGI]['COR_COE'] = (self.RAW_MR[CGI][BB]['S361'] -  self.RAW_MR[CGI][BB]['S369'])*6.0 /  self.ServCellMR[CGI]
                        except ZeroDivisionError:
                            #print('Error occurred when calc Correlate Coe for {}<-{},zero MRs for serving cell'.format(CGI,NCGI))
                            self.logger.warn('Error occurred when calc Correlate Coe for {}<-{},zero MRs for serving cell'.format(CGI,NCGI))
                            self.matchedMR[CGI][NCGI]['COR_COE'] = 0
                        except :
                            #print('Error occurred when calc Correlate Coe for {}<-{}'.format(CGI,NCGI))
                            self.logger.warn('Error occurred when calc Correlate Coe for {}<-{}'.format(CGI,NCGI))
                            self.matchedMR[CGI][NCGI]['COR_COE'] = 0

                        if self.matchedMR[CGI][NCGI]['COR_COE'] > self.CORR_THRES and ((not MatrixBandBased) or self.matchedMR[CGI][NCGI]['COBAND']):
                            try:
                                self.CorrelateMatrixBy12dB[CGI].add(NCGI)
                            except KeyError:
                                self.CorrelateMatrixBy12dB[CGI] = set()
                                self.CorrelateMatrixBy12dB[CGI].add(NCGI)
                            try:
                                self.CorrelateMatrixBy12dB[NCGI].add(CGI)
                            except KeyError:
                                self.CorrelateMatrixBy12dB[NCGI] = set()
                                self.CorrelateMatrixBy12dB[NCGI].add(CGI)

                        try:
                            self.CorrelateMatrixByAppearance[CGI].add(NCGI)
                        except KeyError:
                            self.CorrelateMatrixByAppearance[CGI] = set()
                            self.CorrelateMatrixByAppearance[CGI].add(NCGI)
                        try:
                            self.CorrelateMatrixByAppearance[NCGI].add(CGI)
                        except KeyError:
                            self.CorrelateMatrixByAppearance[NCGI] = set()
                            self.CorrelateMatrixByAppearance[NCGI].add(CGI)

        self.isMrPostProcessed = True
        self.logger.info('reIndexGcellRelation done')

    def _correlateDetect4(self,candidateCGI,preList):
        newClusterDetected = False
        if candidateCGI not in self.CorrelateMatrixBy12dB:
            #如果目标小区没有测量报告信息
            return False
        else:
            for CGI in preList:
                if CGI not in self.CorrelateMatrixBy12dB[candidateCGI]:
                    #如果父级连通簇中的小区不再目标小区的12dB关系中
                    return False
            else:
                #如果父级连通簇中的所有小区都在此小区的12dB关系中,则进一步发觉
                for sCGI in self.CorrelateMatrixBy12dB[candidateCGI]:
                    if sCGI not in preList:
                        if self._correlateDetect4(sCGI,preList+(candidateCGI,)):
                            newClusterDetected = True
                else:
                    if not newClusterDetected:
                        hashid = hash(frozenset(preList+(candidateCGI,)))
                        if not self.hashedMaximalConnClusters.get(hashid):
                            self.hashedMaximalConnClusters[hashid] = set(preList+(candidateCGI,))
                            for CGI in preList+(candidateCGI,):
                                try:
                                    self.CGI2HashedClusters[CGI].append(hashid)
                                except KeyError:
                                    self.CGI2HashedClusters[CGI] = [hashid,]
                        elif self.hashedMaximalConnClusters.get(hashid) and self.hashedMaximalConnClusters.get(hashid) != set(preList+(candidateCGI,)):
                            self.logger.error('Unexpected error,Both {} and {} has same hashid!'.format(set(preList+(candidateCGI,)),self.hashedMaximalConnClusters.get(hashid),hashid))
                            raise ArithmeticError,'Unexpected error,Both {} and {} has same hashid!'.format(set(preList+(candidateCGI,)),self.hashedMaximalConnClusters.get(hashid),hashid)
                return True

    def CalcMaximalConnectCluster(self):
        self.logger.info('Maximal connected cluster calculation started.')
        Count = 0
        maxCount = len(self.CorrelateMatrixBy12dB)
        self.CGI2HashedClusters = {}
        self.hashedMaximalConnClusters = {}
        for CGI in self.CorrelateMatrixBy12dB:
            self._correlateDetect4(CGI,())
            Count += 1
            self.progressQueue.put(100*Count/maxCount)
        self.isMaximalConnectClusterDetected = True
        self.logger.info('Maximal connected cluster calculation completed.')


    def correlateDetec5(self):
        #Fast but error exists.......maximalConnectClusterDetect suggested
        self.logger.info('Correlate 5th cluster check in progress...')
        CGICount = 0
        for CGI in self.CorrelateMatrixBy12dB:
            candidateCluster = []
            for NGI in self.CorrelateMatrixBy12dB[CGI]:
                for sGI in [CGI,NGI]:
                    try:
                        escape = False
                        for hashid in self.CGI2HashedClusters[sGI]:
                            if self.hashedMaximalConnClusters.get(hashid) and set([CGI,NGI]).issubset(self.hashedMaximalConnClusters.get(hashid)):
                                escape = True
                                break
                        if escape:
                            break
                    except KeyError:
                        continue
                else:
                    hashid = hash(frozenset([CGI,NGI]))
                    if hashid not in self.hashedMaximalConnClusters:
                        self.hashedMaximalConnClusters[hashid] = set([CGI,NGI])
                        #self.CorrelateClusters.append(set([CGI,NGI]))
                        for subNCGI in self.hashedMaximalConnClusters[hashid]:
                            try:
                                #self.CGI2Clusters[subNCGI].append(weakref.ref(self.CorrelateClusters[-1]))
                                self.CGI2HashedClusters[subNCGI].append(hashid)
                            except KeyError:
                                #self.CGI2Clusters[subNCGI] = [weakref.ref(self.CorrelateClusters[-1]),]
                                self.CGI2HashedClusters[subNCGI] = [hashid,]

                for hashid in list(self.CGI2HashedClusters[NGI]):
                    try:
                        if CGI not in self.hashedMaximalConnClusters[hashid] and self.hashedMaximalConnClusters[hashid] not in candidateCluster:
                            candidateCluster.append(hashid)
                    except TypeError:
                        self.CGI2HashedClusters[NGI].remove(hashid)

            for hashid in candidateCluster:
                if self.hashedMaximalConnClusters.get(hashid) and self.CorrelateMatrixBy12dB[CGI].issuperset(self.hashedMaximalConnClusters.get(hashid)):
                    newCluster = self.hashedMaximalConnClusters.get(hashid).copy()
                    newCluster.add(CGI)
                    #print('cluster {} expanded to {}'.format(cluster,newCluster))
                    if hash(frozenset(newCluster)) not in self.hashedMaximalConnClusters:
                        #print('added')
                        self.hashedMaximalConnClusters[hash(frozenset(newCluster))] = newCluster
                        for subNCGI in newCluster:
                            try:
                                self.CGI2HashedClusters[subNCGI].append(hash(frozenset(newCluster)))
                            except KeyError:
                                self.CGI2HashedClusters[subNCGI] = [hash(frozenset(newCluster)),]

                        for tCGI in newCluster:
                            for shashid in list(self.CGI2HashedClusters[tCGI]):
                                if self.hashedMaximalConnClusters.get(shashid) and not( self.hashedMaximalConnClusters.get(shashid) == newCluster) and newCluster.issuperset(self.hashedMaximalConnClusters.get(shashid)):
                                    for lcgi in self.hashedMaximalConnClusters[shashid]:
                                        self.CGI2HashedClusters[lcgi].remove(shashid)
                                    del self.hashedMaximalConnClusters[shashid]
                elif self.hashedMaximalConnClusters.get(shashid):
                    #continue
                    intersection = self.CorrelateMatrixBy12dB[CGI].intersection(self.hashedMaximalConnClusters.get(shashid))
                    if intersection:
                        intersection.add(CGI)
                        for tCGI in intersection:
                            for shashid in self.CGI2HashedClusters[tCGI]:
                                if self.hashedMaximalConnClusters.get(shashid)  and (intersection.issubset(self.hashedMaximalConnClusters.get(shashid)) or intersection.issuperset(self.hashedMaximalConnClusters.get(shashid))):
                                    break
                            else:
                                continue
                            break
                        else:
                            if hash(frozenset(intersection)) not in self.hashedMaximalConnClusters:
                                #print('cluster {} splited to {}'.format(ref(),intersection))
                                self.hashedMaximalConnClusters[hash(frozenset(intersection))] = intersection
                                for subNCGI in intersection:
                                    try:
                                        self.CGI2HashedClusters[subNCGI].append( hash(frozenset(intersection)))
                                    except KeyError:
                                        self.CGI2HashedClusters[subNCGI] = [ hash(frozenset(intersection)),]
                            del intersection
            CGICount+=1
            self.progressQueue.put(CGICount*100/len(self.CorrelateMatrixBy12dB))
                #self.logger.info('Length of CorrlateCGIS:{}, processed cell {} {:.2%}, candidate this loop:{}.'.format(len(self.CorrelateClusters),CGICount,CGICount*1.0/len(self.CorrelateMatrixBy12dB),len(candidateCluster)))
                #print('Length of CorrlateCGIS:{}, processed cell {} {:.2%}, candidate this loop:{}.'.format(len(CorrelateClusters),CGICount,CGICount*1.0/len(self.CorrelateMatrixBy12dB),len(candidateCluster)))

        for CGI in self.CGI2HashedClusters:
            for hashid in list(self.CGI2HashedClusters[CGI]):
                if not self.hashedMaximalConnClusters.get(hashid):
                    self.CGI2HashedClusters[CGI].remove(hashid)

        self.isMaximalConnectClusterDetected = True
        self.logger.info('Correlate cluster check done.')

    def correlateDetect3(self):
        self.logger.info('Correlate cluster check in progress...')
        CGICount = 0
        for CGI in self.CorrelateMatrixBy12dB:
            candidateCluster = []
            for NGI in self.CorrelateMatrixBy12dB[CGI]:
                for sGI in [CGI,NGI]:
                    try:
                        escape = False
                        for ref in self.CGI2Clusters[sGI]:
                            if ref() and set([CGI,NGI]).issubset(ref()):
                                escape = True
                                break
                        if escape:
                            break
                    except KeyError:
                        continue
                else:
                    if set([CGI,NGI]) not in self.CorrelateClusters:
                        self.CorrelateClusters.append(set([CGI,NGI]))
                        for subNCGI in self.CorrelateClusters[-1]:
                            try:
                                self.CGI2Clusters[subNCGI].append(weakref.ref(self.CorrelateClusters[-1]))
                            except KeyError:
                                self.CGI2Clusters[subNCGI] = [weakref.ref(self.CorrelateClusters[-1]),]

                for ref in list(self.CGI2Clusters[NGI]):
                    try:
                        if CGI not in ref() and ref() not in candidateCluster:
                            candidateCluster.append(ref)
                    except TypeError:
                        self.CGI2Clusters[NGI].remove(ref)

            for ref in candidateCluster:
                if ref() and self.CorrelateMatrixBy12dB[CGI].issuperset(ref()):
                    newCluster = ref().copy()
                    newCluster.add(CGI)
                    #print('cluster {} expanded to {}'.format(cluster,newCluster))
                    if newCluster not in self.CorrelateClusters:
                        #print('added')
                        self.CorrelateClusters.append(newCluster)
                        for subNCGI in newCluster:
                            try:
                                self.CGI2Clusters[subNCGI].append(weakref.ref(self.CorrelateClusters[-1]))
                            except KeyError:
                                self.CGI2Clusters[subNCGI] = [weakref.ref(self.CorrelateClusters[-1]),]
                            #                    try:
                            #                        if ref():
                            #                            CorrelateCGIs.remove(ref())
                        for tCGI in newCluster:
                            for sref in self.CGI2Clusters[tCGI]:
                                if sref() and not( sref() == newCluster) and newCluster.issuperset(sref()):
                                    self.CorrelateClusters.remove(sref())
                                #                    except ValueError:
                                #                        print('Cluster {} already removed from CorrelateCGIs.'.format(ref))
                elif ref():
                    #continue
                    intersection = self.CorrelateMatrixBy12dB[CGI].intersection(ref())
                    if intersection:
                        intersection.add(CGI)
                        for tCGI in intersection:
                            for sref in self.CGI2Clusters[tCGI]:
                                if sref()  and (intersection.issubset(sref()) or intersection.issuperset(sref())):
                                    break
                            else:
                                continue
                            break
                        else:
                            if intersection not in self.CorrelateClusters:
                                #print('cluster {} splited to {}'.format(ref(),intersection))
                                self.CorrelateClusters.append(intersection)
                                for subNCGI in intersection:
                                    try:
                                        self.CGI2Clusters[subNCGI].append(weakref.ref(self.CorrelateClusters[-1]))
                                    except KeyError:
                                        self.CGI2Clusters[subNCGI] = [weakref.ref(self.CorrelateClusters[-1]),]
                            del intersection
            CGICount+=1
            #if CGICount*100/len(self.CorrelateMatrixBy12dB) % 10 == 0 and (CGICount-1)*100/len(self.CorrelateMatrixBy12dB) % 10 != 0:
            self.progressQueue.put(CGICount*100/len(self.CorrelateMatrixBy12dB))
                #self.logger.info('Length of CorrlateCGIS:{}, processed cell {} {:.2%}, candidate this loop:{}.'.format(len(self.CorrelateClusters),CGICount,CGICount*1.0/len(self.CorrelateMatrixBy12dB),len(candidateCluster)))
                #print('Length of CorrlateCGIS:{}, processed cell {} {:.2%}, candidate this loop:{}.'.format(len(CorrelateClusters),CGICount,CGICount*1.0/len(self.CorrelateMatrixBy12dB),len(candidateCluster)))

        for CGI in self.CGI2Clusters:
            for ref in list(self.CGI2Clusters[CGI]):
                if not ref() or ref() not in self.CorrelateClusters:
                    self.CGI2Clusters[CGI].remove(ref)
        self.maximalConnectClusterDetected3 = True
        self.logger.info('Correlate cluster check done.')
        #self.logger.info('correlateDetect3 Done.')

    def ClusterShrinkCHK(self):
        for cluter in self.CorrelateClusters:
            for CGI in cluter:
                for ref in self.CGI2Clusters[CGI]:
                    if ( ref().issuperset(cluter) or ref().issubset(cluter) ) and  ref() != cluter:
                        self.logger.info('cluster {} and cluster {}'.format(cluter,ref()))
        return

    def CalcICGI2CGIIntfMRs(self,CGI,ICGI):
        'Calculate ICGI to CGI interferance'
        try:
            CoFreqs = len(set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[ICGI]['TRX'].keys()))
        except KeyError:
            self.logger.warn('CRITICAL ERROR:Cell {} or {} without TRX attr'.format(CGI,ICGI))
            CoFreqs = 0
            #raise

        try:
            LocalCoFreqERL = CoFreqs*1.0/len(self.GCELL[CGI]['TRX']) * self.ServCellMR[CGI] / 6
        except KeyError:
            LocalCoFreqERL = 0

        try:
            RemoteCoFreqERL = CoFreqs*1.0/len(self.GCELL[ICGI]['TRX']) * self.ServCellMR[ICGI] /6
        except KeyError:
            RemoteCoFreqERL = 0

        try:
            #print(CoFreqs,LocalCoFreqERL,RemoteCoFreqERL,self.matchedMR[CGI][ICGI]['S361'],self.matchedMR[CGI][ICGI]['S369'],(self.matchedMR[CGI][ICGI]['S361'] - self.matchedMR[CGI][ICGI]['S369']) * 6.0 / self.ServCellMR[CGI]  * RemoteCoFreqERL)
            return min(LocalCoFreqERL,(self.matchedMR[CGI][ICGI]['S361'] - self.matchedMR[CGI][ICGI]['S369']) * 6.0 / self.ServCellMR[CGI]  * RemoteCoFreqERL)
        except KeyError:
            if (not self.GCELL[CGI].get('EXTCELL')) and (CGI not in self.ServCellMR or CGI not in self.matchedMR):
                warn = 'No reported MR in self.matchedMR or ServCellMR, Investigation needed for {}'.format(CGI)
                if warn not in self.EscapedWarn:
                    self.EscapedWarn.append(warn)
                    self.logger.warn('No reported MR in self.matchedMR or ServCellMR, Investigation needed for {}'.format(CGI))
            return 0

    def CalcClusterIntfMRs2(self,cluster):
        intfMRs = 0.0
        for CGI in cluster:
            intfCGIs = list(cluster)
            intfCGIs.remove(CGI)
            for ICGI in intfCGIs:
                intfMRs += self.CalcICGI2CGIIntfMRs(CGI,ICGI)
        return  intfMRs


    def CalcCoArfcnAffectingOthers(self,CGI):
        AffectingMRs = 0
        CalcedCoArfcns = []
        if CGI in self.CorrelateMatrixBy12dB:
            for NCGI in self.CorrelateMatrixBy12dB:
                try:
                    CalcedCoArfcns.extend([arfcn for arfcn in  set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[NCGI]['TRX'].keys()) if arfcn not in CalcedCoArfcns])
                except KeyError:
                    self.logger.warn('CRITICAL ERROR:Cell {} or {} without TRX attr'.format(CGI,NCGI))
                AffectingMRs += self.CalcICGI2CGIIntfMRs(NCGI,CGI)

        try:
            LocalCoFreqERL = len(CalcedCoArfcns)*1.0/len(self.GCELL[CGI]['TRX']) * self.ServCellMR[CGI] / 6
        except KeyError:
            LocalCoFreqERL = 0

        return min(AffectingMRs,LocalCoFreqERL)

    def CalcCoArfcnAffectedByOthers(self,CGI):
        AffectingMRs = 0
        CalcedCoArfcns = []
        if CGI in self.CorrelateMatrixBy12dB:
            for NCGI in self.CorrelateMatrixBy12dB:
                try:
                    CalcedCoArfcns.extend([arfcn for arfcn in  set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[NCGI]['TRX'].keys()) if arfcn not in CalcedCoArfcns])
                except KeyError:
                    self.logger.warn('CRITICAL ERROR:Cell {} or {} without TRX attr'.format(CGI,NCGI))
                AffectingMRs += self.CalcICGI2CGIIntfMRs(CGI,NCGI)
        try:
            LocalCoFreqERL = len(CalcedCoArfcns)*1.0/len(self.GCELL[CGI]['TRX']) * self.ServCellMR[CGI] / 6
        except KeyError:
            LocalCoFreqERL = 0

        return min(AffectingMRs,LocalCoFreqERL)

    def CalcCGIInferior12dBtoICGIMRs(self,CGI,ICGI):
        '''
        计算CGI做为服务小区时相对于ICGI电平强度不大于12dB的实际话务量测量报告数
        每六个测量报告算一次
        '''
        try:
            return (self.matchedMR[CGI][ICGI]['S361'] - self.matchedMR[CGI][ICGI]['S369'])/6.0
        except KeyError:
            return 0

    def CalcClusterInferiorMRsBy12dB(self,cluster):
        '''
        当前簇内小区服务电平强度差不满足大于12dB测量报告数
        '''
        intfMRs = 0.0
        for CGI in cluster:
            intfCGIs = list(cluster)
            intfCGIs.remove(CGI)
            for ICGI in intfCGIs:
                intfMRs += self.CalcCGIInferior12dBtoICGIMRs(CGI,ICGI)
        return intfMRs

    def CalcServCellInferiortoNeisBy12dB(self,CGI):
        """
        计算服务小区相对于其所有邻区中电平强度不大于12dB的测量报告数
        """
        mr = 0
        if CGI in self.matchedMR:
            for ICGI in self.matchedMR[CGI]:
                mr += self.CalcCGIInferior12dBtoICGIMRs(CGI,ICGI)
        return mr

    def CalcServCellIntfoNeisBy12dB(self,CGI):
        """
        计算服务小区的所有邻区的测量报告中中，服务小区电平相对强度大于12dB的测量报告数，且比例大于CORR_THRES
        越区覆盖指标
        """
        mr = 0

        if CGI in self.CorrelateMatrixBy12dB:
            for ICGI in self.CorrelateMatrixBy12dB[CGI]:
                mr += self.CalcCGIInferior12dBtoICGIMRs(ICGI,CGI) * 6

        return mr


    def CalcServCellOverlappedNeisBy12dB(self,CGI):
        """
        计算服务小区的所有同频邻区的测量报告中中，服务小区电平相对强度大于12dB的测量报告数，且比例大于CORR_THRES的邻区数，同时要求服务小区测量报告在该邻小区中占比大于OVERLAP_COVER_THRES
        越区覆盖指标，共站小区不计入
        """
        overlapedCells = [] #仅根据测量报告判决的过覆盖小区
        overlapedTRXs = 0
        overlapedCellsBeyondDelNei = []
        overlapedTRXsBeyondDelNei = 0

        if CGI in self.CorrelateMatrixBy12dB:
            for ICGI in self.CorrelateMatrixBy12dB[CGI]:
                try:
                    if  self.matchedMR[ICGI][CGI]['COR_COE'] > self.OVERLAP_COR_THRES and self.matchedMR[ICGI][CGI]['S361']*1.0 / self.ServCellMR[ICGI] > self.OVERLAP_COVER_THRES:
                        try:
                            overlapedTRXs += len(self.GCELL[ICGI]['TRX'])
                        except KeyError:
                            pass
                        #                    if CGI in self.CGI2GEO and ICGI in self.CGI2GEO and self.CGI2GEO[CGI] == self.CGI2GEO[ICGI]:
                        #                        #覆盖不计共站小区
                        #                        continue
                        #                    else:
                        #                        if ICGI not in overlapedCells:
                        #                            overlapedCells.append(ICGI)
                        #                        if ICGI not in overlapedCellsBeyondDelNei:
                        #                            if CGI in self.CGI2GEO and ICGI in self.CGI2GEO and self.CGI2GEO[ICGI] not in self.GEOSets[self.CGI2GEO[CGI]]['neis']:
                        #                                #仅统计非泰森邻区的越区覆盖小区
                        #                                if self.CGI2GEO[ICGI] in self.GEOSets and ICGI in self.GEOSets[self.CGI2GEO[ICGI]]['cell'] and  self.GEOSets[self.CGI2GEO[ICGI]]['cell'][ICGI]['type'] not in ['indoor','underlayer']:
                        #                                    #由于室分系统和底层网不纳入站址计算，所以对他们越区覆盖也不进行考虑
                        #                                    overlapedCellsBeyondDelNei.append(ICGI)
                        if ICGI not in overlapedCells:
                            #竞争小区
                            overlapedCells.append(ICGI)

                        if CGI in self.CGI2GEO and ICGI in self.CGI2GEO and self.CGI2GEO[CGI] == self.CGI2GEO[ICGI]:
                            #过覆盖不计共站小区
                            continue
                        if ICGI not in overlapedCellsBeyondDelNei:
                            if CGI in self.CGI2GEO and ICGI in self.CGI2GEO and self.CGI2GEO[ICGI] not in self.GEOSets[self.CGI2GEO[CGI]]['neis']:
                                #仅统计非泰森邻区的越区覆盖小区
                                if self.CGI2GEO[ICGI] in self.GEOSets and ICGI in self.GEOSets[self.CGI2GEO[ICGI]]['cell'] and  self.GEOSets[self.CGI2GEO[ICGI]]['cell'][ICGI]['type'] not in ['indoor','underlayer']:
                                    #由于室分系统和底层网不纳入站址计算，所以对他们越区覆盖也不进行考虑
                                    overlapedCellsBeyondDelNei.append(ICGI)
                                    try:
                                        overlapedTRXsBeyondDelNei += len(self.GCELL[ICGI]['TRX'])
                                    except KeyError:
                                        pass

                except KeyError:
                    pass
                except ZeroDivisionError:
                    pass
        return overlapedCells,overlapedCellsBeyondDelNei,overlapedTRXs,overlapedTRXsBeyondDelNei


    def CalcCellIntfMRsInCluster(self,observeCGI,cluster):
        "计算observe在该簇中存干扰的话务量，有可能会大于簇实际的话务量？"
        donePair = []
        intfMRs = 0.0
        intfedCGIs = list(cluster)
        intfedCGIs.remove(observeCGI)
        for CGI in intfedCGIs:
            intfMRs += self.CalcICGI2CGIIntfMRs(CGI,observeCGI)

        return  intfMRs

    def CalcCoArfcnConflictByNetwork(self,checkType = 'ALL'):
        self.logger.info('Detect ARFCN conflict started...')
        #计算12dB同频干扰
        processedPair = []
        for CGI in self.GCELL:
            #Init Co.Adj Arfcn info
            try:
                del self.GCELL[CGI]['CoArfcnCell']
            except KeyError:
                pass
        count = 0
        maxCount = len(self.CorrelateMatrixBy12dB)
        for CGI in self.CorrelateMatrixBy12dB:
            for ICGI in self.CorrelateMatrixBy12dB[CGI]:
                if  CGI in self.GCELL and ICGI in self.GCELL:
                    if set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[ICGI]['TRX'].keys()):
                        try:
                            self.GCELL[CGI]['CoArfcnCell'].add(ICGI)
                        except KeyError:
                            if CGI in self.GCELL:
                                self.GCELL[CGI]['CoArfcnCell'] = set()
                                self.GCELL[CGI]['CoArfcnCell'].add(ICGI)

                        try:
                            self.GCELL[ICGI]['CoArfcnCell'].add(CGI)
                        except KeyError:
                            if ICGI in self.GCELL:
                                self.GCELL[ICGI]['CoArfcnCell'] = set()
                                self.GCELL[ICGI]['CoArfcnCell'].add(CGI)

            count+=1
            self.progressQueue.put(count*100/maxCount)
        self.logger.info('Detect ARFCN conflict completed.')
        return True

    def CalcARFCNConflictByNetwork(self,escapingAdj = False):
        #Speed by escaping hist record
        self.logger.info('Detect ARFCN conflict started...')
        #计算12dB同频干扰
        for CGI in self.GCELL:
            #Init Co.Adj Arfcn info
            try:
                del self.GCELL[CGI]['CoArfcnCell']
            except KeyError:
                pass
            try:
                del self.GCELL[CGI]['CoSiteAdjArfcnCell']
            except KeyError:
                pass
        count = 0
        maxCount = len(self.CorrelateMatrixBy12dB)
        for CGI in self.CorrelateMatrixBy12dB:
            for ICGI in self.CorrelateMatrixBy12dB[CGI]:
                if CGI in self.GCELL and ICGI in self.GCELL:
                    if set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[ICGI]['TRX'].keys()):
                        try:
                            self.GCELL[CGI]['CoArfcnCell'].add(ICGI)
                        except KeyError:
                            if CGI in self.GCELL:
                                self.GCELL[CGI]['CoArfcnCell'] = set()
                                self.GCELL[CGI]['CoArfcnCell'].add(ICGI)
                        try:
                            self.GCELL[ICGI]['CoArfcnCell'].add(CGI)
                        except KeyError:
                            if ICGI in self.GCELL:
                                self.GCELL[ICGI]['CoArfcnCell'] = set()
                                self.GCELL[ICGI]['CoArfcnCell'].add(CGI)
            count+=1
            #print(count*100/maxCount)
            try:
                self.progressQueue.put_nowait(count*100/maxCount)
            except:
                pass
        self.logger.info('Detect coARFCN conflict Done')

        if escapingAdj:
            return True

        #计算共站邻频干扰
        count = 0
        maxCount = len(self.GEOSets)
        for geo in self.GEOSets:
            coSiteCells = self.GEOSets[geo]['cell'].keys()
            for CGI in coSiteCells:
                intfCGIs = coSiteCells[:]
                intfCGIs.remove(CGI)
                for ICGI in intfCGIs:
                    if  CGI in self.GCELL and ICGI in self.GCELL:
                        arfcns = [int(arfcn) for arfcn in self.GCELL[CGI]['TRX'].keys()] + [int(arfcn) for arfcn in self.GCELL[ICGI]['TRX'].keys()]
                        for idx in range(0,len(arfcns)):
                            for iidx in range(idx+1,len(arfcns)):
                                if abs(arfcns[idx] - arfcns[iidx]) < 2:
                                    try:
                                        self.GCELL[CGI]['CoSiteAdjArfcnCell'].add(ICGI)
                                    except KeyError:
                                        if CGI in self.GCELL:
                                            self.GCELL[CGI]['CoSiteAdjArfcnCell'] = set()
                                            self.GCELL[CGI]['CoSiteAdjArfcnCell'].add(ICGI)
                                    try:
                                        self.GCELL[ICGI]['CoSiteAdjArfcnCell'].add(CGI)
                                    except KeyError:
                                        if CGI in self.GCELL:
                                            self.GCELL[ICGI]['CoSiteAdjArfcnCell'] = set()
                                            self.GCELL[ICGI]['CoSiteAdjArfcnCell'].add(CGI)
            count+=1
            #print(count*100/maxCount)
            try:
                self.progressQueue.put_nowait(count*100/maxCount)
            except:
                pass

        self.logger.info('Detect adjARFCN conflict Done')
        return True

    def __CalcARFCNConflictByNetwork(self,escapingAdj = False):
        self.logger.info('Detect ARFCN conflict started...')
        #计算12dB同频干扰
        processedPair = []
        for CGI in self.GCELL:
            #Init Co.Adj Arfcn info
            try:
                del self.GCELL[CGI]['CoArfcnCell']
            except KeyError:
                pass
            try:
                del self.GCELL[CGI]['CoSiteAdjArfcnCell']
            except KeyError:
                pass
        count = 0
        maxCount = len(self.CorrelateMatrixBy12dB)
        for CGI in self.CorrelateMatrixBy12dB:
            for ICGI in self.CorrelateMatrixBy12dB[CGI]:
                if set([CGI,ICGI]) not in processedPair and CGI in self.GCELL and ICGI in self.GCELL:

                    if set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[ICGI]['TRX'].keys()):
                        try:
                            self.GCELL[CGI]['CoArfcnCell'].add(ICGI)
                        except KeyError:
                            if CGI in self.GCELL:
                                self.GCELL[CGI]['CoArfcnCell'] = set()
                                self.GCELL[CGI]['CoArfcnCell'].add(ICGI)
                        try:
                            self.GCELL[ICGI]['CoArfcnCell'].add(CGI)
                        except KeyError:
                            if ICGI in self.GCELL:
                                self.GCELL[ICGI]['CoArfcnCell'] = set()
                                self.GCELL[ICGI]['CoArfcnCell'].add(CGI)
                    processedPair.append(set([CGI,ICGI]))
            count+=1
            self.progressQueue.put(count*100/maxCount)

        if escapingAdj:
            return True

        #计算共站邻频干扰
        for geo in self.GEOSets:
            processedPair = []
            coSiteCells = self.GEOSets[geo]['cell'].keys()
            for CGI in coSiteCells:
                intfCGIs = coSiteCells[:]
                intfCGIs.remove(CGI)
                for ICGI in intfCGIs:
                    if set([CGI,ICGI]) not in processedPair and CGI in self.GCELL and ICGI in self.GCELL:
                        arfcns = [int(arfcn) for arfcn in self.GCELL[CGI]['TRX'].keys()] + [int(arfcn) for arfcn in self.GCELL[ICGI]['TRX'].keys()]
                        for idx in range(0,len(arfcns)):
                            for iidx in range(idx+1,len(arfcns)):
                                if abs(arfcns[idx] - arfcns[iidx]) < 2:
                                    try:
                                        self.GCELL[CGI]['CoSiteAdjArfcnCell'].add(ICGI)
                                    except KeyError:
                                        if CGI in self.GCELL:
                                            self.GCELL[CGI]['CoSiteAdjArfcnCell'] = set()
                                            self.GCELL[CGI]['CoSiteAdjArfcnCell'].add(ICGI)

                                    try:
                                        self.GCELL[ICGI]['CoSiteAdjArfcnCell'].add(CGI)
                                    except KeyError:
                                        if CGI in self.GCELL:
                                            self.GCELL[ICGI]['CoSiteAdjArfcnCell'] = set()
                                            self.GCELL[ICGI]['CoSiteAdjArfcnCell'].add(CGI)
                    processedPair.append(set([CGI,ICGI]))
        self.logger.info('Detect ARFCN conflict Done')
        return True

    def ConflictTCHResolver(self,AvailARFCNList,ResolverCells):
        #global FailedTryCellSets
        #global MAX_CONFLICT_RESOLVE_DEPTH
        log = logging.getLogger('global')
        def _ArfcnReplace(CGI,orgArfcn,newArfcn):

            if CGI in self.ARFCN_IDX.get(newArfcn,[]):
                raise Exception('ARFCN {} already linked with CGI {}'.format(newArfcn,CGI))

            try:
                self.ARFCN_IDX[orgArfcn].remove(CGI)
            except :
                raise

            try:
                self.ARFCN_IDX[newArfcn].append(CGI)
            except KeyError:
                self.ARFCN_IDX[newArfcn] = [CGI,]

            try:
                if newArfcn in self.GCELL[CGI]['TRX']:
                    raise Exception('CGI {} already used ARFCN {} '.format(CGI,newArfcn))
                self.GCELL[CGI]['TRX'][newArfcn] = self.GCELL[CGI]['TRX'][orgArfcn]
                del self.GCELL[CGI]['TRX'][orgArfcn]
            except :
                raise

            return True

        def _ConflictTCHResolver(ArfcnList,ResolverCells,leaseArfcn,LockedCell,targetingCell,tryDepth = 0):
            #global FailedTryCellSets

            '''
            尝试对targetingCell中的leaseArfcn频点进行退让，直至达到最大迭代次数，操作不可逆
            '''
            log = logging.getLogger('global')
            msg = '{}:Resolving CGI {},depth {} start.'.format(LockedCell+[targetingCell,],targetingCell,tryDepth)
            self.logger.debug(msg)
            #print()

            if leaseArfcn:
                msg = '{}:retriving arfcn {},depth {}.'.format(LockedCell+[targetingCell,],leaseArfcn,tryDepth)
                self.logger.debug(msg)
                #print()
                if '{}'.format(leaseArfcn) not in self.GCELL[targetingCell]['TRX']:
                    msg = '{}:retriving arfcn {},depth {}.But CGI {} do not have that arfcn.'.format(LockedCell+[targetingCell,],leaseArfcn,tryDepth,targetingCell)
                    #print()
                    self.logger.warning(msg)

            if targetingCell not in self.GCELL:
                msg = '{}:Resolving CGI {},depth {} aborted, CGI not in GCELL list.'.format(LockedCell+[targetingCell,],targetingCell,tryDepth)
                #print()
                self.logger.warning(msg)
                return False

            for arfcn in list(self.GCELL[targetingCell]['TRX'].keys()):
                if arfcn not in self.GCELL[targetingCell]['TRX']:
                    #当该频点已被替换时，不再进行检查
                    #而替换后的新频点理论上不再会是有冲突的频点
                    msg = 'Unexpected replaced trx in CGI {}'.format(targetingCell)
                    #print()
                    self.logger.warning(msg)
                    continue

                forbidListBy12dB = set()
                if targetingCell in self.CorrelateMatrixBy12dB:
                    for ICGI in self.CorrelateMatrixBy12dB[targetingCell]:
                        if ICGI not in self.GCELL:
                            continue
                        for iarfcn in self.GCELL[ICGI]['TRX'].keys():
                            forbidListBy12dB.add(int(iarfcn))
                elif targetingCell not in self.ServCellMR:
                    msg = '{}:Resolving CGI {},depth {} aborted, CGI do not have MR data.'.format(LockedCell+[targetingCell,],targetingCell,tryDepth)
                    #print()
                    self.logger.warning(msg)
                    return False

                if leaseArfcn:
                    #将需要退让的频点列入禁用频点列表
                    forbidListBy12dB.add(leaseArfcn)

                forbidListByCoSite = set()
                if targetingCell in self.CGI2GEO:
                    coSiteCells = self.GEOSets[self.CGI2GEO[targetingCell]]['cell'].keys()
                    #coSiteCells.remove(targetingCell)
                    for ICGI in coSiteCells:
                        if ICGI in self.GCELL:
                            for iarfcn in self.GCELL[ICGI]['TRX'].keys():
                                if ICGI != targetingCell:
                                    forbidListBy12dB.add(int(iarfcn))
                                    forbidListByCoSite.add(int(iarfcn)-1)
                                    forbidListByCoSite.add(int(iarfcn)+1)
                                elif ICGI == targetingCell:
                                    forbidListByCoSite.add(int(iarfcn)-1)
                                    forbidListByCoSite.add(int(iarfcn)+1)


                AvailARFCNs = ArfcnList.difference(forbidListBy12dB).difference(forbidListByCoSite)

                if int(arfcn) in AvailARFCNs:
                    AvailARFCNs.discard(int(arfcn))
                elif arfcn == self.GCELL[targetingCell]['BCCH']:
                    AvailARFCNs.discard(int(arfcn))
                elif int(arfcn) in forbidListBy12dB or int(arfcn) in forbidListByCoSite:
                    msg = '{}:Resolving CGI {} conflict arfcn {}'.format(LockedCell+[targetingCell,],targetingCell,arfcn)
                    #print()
                    self.logger.debug(msg)
                    try:
                        replaced = [AvailARFCNs.pop(),]
                        while '{}'.format(replaced[-1]) in self.GCELL[targetingCell]['TRX']:
                        #对于本小区已分配的合理频点，这里不再次分配,仅可能减少现网数据变动
                        #一直轮询，直至找到一个当前小区未使用的可用频点
                            replaced.append(AvailARFCNs.pop())

                        _ArfcnReplace(targetingCell,arfcn,'{}'.format(replaced[-1]))
                        msg = '{}:CGI {} change arfcn {} to {} from pure avail arfcns.'.format(LockedCell+[targetingCell,],targetingCell,arfcn,replaced[-1])
                        #print()
                        self.logger.debug(msg)
                        del replaced[-1]
                    except KeyError:
                    #如果轮询完所有频点均不可用，则开始退让频点操作
                        msg = '{}:CGI {} no pure avail arfcn change for {}, adjancent retrive start .'.format(LockedCell+[targetingCell,],targetingCell,arfcn)
                        self.logger.debug(msg)
                        #Catch AvailARFCNs.pop() error, when empty raise KeyError
                        if tryDepth < self.MAX_CONFLICT_RESOLVE_DEPTH:
                            forbidArfcnAffects = []
                            for forbidArfcn in list(forbidListBy12dB):
                                if '{}'.format(forbidArfcn) in self.GCELL[targetingCell]['TRX']:
                                    #对于当前小区中已用频点，不再尝试
                                    continue
                                #                            if forbidArfcn in [int(trx) - 1 for trx in self.GCELL[targetingCell]['TRX'] ] + [int(trx) + 1 for trx in self.GCELL[targetingCell]['TRX'] ]:
                                #                                #对于当前小区中已用频点的邻频频点，不再尝试
                                #                                continue
                                if forbidArfcn in forbidListByCoSite:
                                    #对于共站（包含本小区）已用频点的邻频频点，不再尝试
                                    continue
                                if forbidArfcn not in ArfcnList:
                                    #对于非本频段可用频点，不再尝试
                                    continue
                                if forbidArfcn == leaseArfcn:
                                    # 对于当前需要退让的频点，不再尝试
                                    continue
                                #寻找可用的已占频点中选择与当前小区有相关性且包含该频点的小区和频点列表
                                affectCGI = set(self.ARFCN_IDX.get('{}'.format(forbidArfcn),[])).intersection(set(list(self.CorrelateMatrixBy12dB[targetingCell])+coSiteCells))
                                forbidArfcnAffects.append((forbidArfcn,affectCGI))
                            #排序后选择一个影响最小的频点（涉及的小区最少）
                            forbidArfcnAffects.sort(key = lambda x : len(x[1]))

                            candidateArfcn = None
                            for candidatePair in forbidArfcnAffects:
                                candidateArfcn = '{}'.format(candidatePair[0]) #需要退让的频点
                                if [cgi for cgi in candidatePair[1] if cgi not in ResolverCells]:
                                    #该退让频点存在改频范围之外的小区内，不做处理,换下一个频点进行退让
                                    continue
                                if [cgi for cgi in candidatePair[1] if cgi in LockedCell+[targetingCell,]]:
                                    #对于当前小区和上一层正在处理的小区，不做处理,换下一个频点进行退让
                                    continue

                                for cgi in candidatePair[1]:
                                    if set(LockedCell+[targetingCell,cgi]) in self.FailedTryCellSets:
                                        #如果LockedCell+[targetingCell,cgi]这一组小区已经尝试退让并失败过，本次不再进行尝试
                                        break
                                    if not _ConflictTCHResolver(ArfcnList,ResolverCells,candidatePair[0],LockedCell+[targetingCell,],cgi,tryDepth+1):
                                        #如果当前cgi无法完成candidatePair[0]频点的退让，则跳出当前频点退让，换下组频点尝试
                                        #同时，将该组小区加入失败尝试记录，在本级根节点小区的改频迭代中不再尝试
                                        self.FailedTryCellSets.append(set(LockedCell+[targetingCell,cgi]))
                                        break
                                else:
                                    #如果candidatePair[1]中所有小区均的顺利完成频点candidatePair[0]退让，则不再对剩余的频点进行退让尝试
                                    #直接转至if candidateArfcn处，对当前小区频点替换退让出来的频点
                                    break
                            else:
                                #如果所有的频点退让尝试都失败，则本小区的退让尝试失败
                                """
                                主要的失败检测
                                """
                                msg = '{}:Resolving CGI {},depth {} failed with all forbid arfcns.'.format(LockedCell+[targetingCell,],targetingCell,tryDepth)
                                #print()
                                self.logger.debug(msg)
                                return False

                            if candidateArfcn:
                                _ArfcnReplace(targetingCell,arfcn,candidateArfcn)
                                msg = '{}:CGI {} change arfcn {} to {} by affecting other cells,stop at depth {} arfcn {}.'.format(LockedCell+[targetingCell,],targetingCell,arfcn,candidateArfcn,tryDepth,arfcn)
                                #print()
                                self.logger.debug(msg)
                            else:
                                #TODO
                                #理论上这种情况不应该出现
                                raise Exception
                        else:
                            msg = '{}:Reject resolve CGI {} any further for retrive ARFCN {},max depth {} reached .'.format(LockedCell+[targetingCell,],targetingCell,leaseArfcn,tryDepth)
                            #print()
                            self.logger.debug(msg)
                            msg= '{}:Resolving CGI {},depth {} failure.'.format(LockedCell+[targetingCell,],targetingCell,tryDepth)
                            #print()
                            self.logger.debug(msg)
                            return False
                    finally:
                        #将本小区的可用频点重新填充回可用频点列表中
                        try:
                            for passedArfcn in replaced:
                                AvailARFCNs.add(passedArfcn)
                        except UnboundLocalError:
                            pass
                else:
                    #MultiChoiceCell[CGI].append(int(arfcn))
                    msg = '{}:CGI {} used ARFCN {} but not in avail arfcn list,why?'.format(LockedCell+[targetingCell,],targetingCell,arfcn)
                    #print()
                    self.logger.warning(msg)
            else:
                #完成该小区的所有频点轮询后，只要不是冲突解决失败或是无效配置数据，均返回成功回退结果
                msg = '{}:Resolving CGI {},depth {} done.'.format(LockedCell+[targetingCell,],targetingCell,tryDepth)
                #print()
                self.logger.debug(msg)
                return True

            raise Exception('Unexpected error')
            return False

        msg = 'Conflict TCH resolving started...'
        #print()
        self.logger.warning(msg)
        LockedCell = {}
        #refLockedCell = weakref.ref(LockedCell)

        progressCount = 0
        maxProgress = len(ResolverCells)
        for CGI in ResolverCells:
            progressCount+=1
            self.FailedTryCellSets = [] #每处理一个小区，对历史失败小区组记录进行清空
            ArfcnList = set(AvailARFCNList)
            if CGI not in self.GCELL:
                continue

            if int(self.GCELL[CGI].get('BCCH')) in range(0,125) or int(self.GCELL[CGI].get('BCCH')) in range(1000,1025):
                ArfcnList = ArfcnList.intersection(set(range(0,125)+range(1000,1025)))
            elif int(self.GCELL[CGI].get('BCCH')) in range(512,850):
                ArfcnList = ArfcnList.intersection(set(range(512,850)))

            if not self.GCELL[CGI].get('CoArfcnCell',[] ) and not self.GCELL[CGI].get('CoSiteAdjArfcnCell',[] ):
            #无问题小区不做处理
                continue

            if not _ConflictTCHResolver(ArfcnList,ResolverCells,None,[],CGI):
                msg= 'Failed to resolve CGI {} with max try depth'.format(CGI)
                #print()
                self.logger.warning(msg)
            else:
                self.logger.info('CGI {} resolve success'.format(CGI))
            self.progressQueue.put(progressCount*100/maxProgress)

        msg = 'Conflict TCH resolving done.'
        #print()
        self.logger.info(msg)
        return True

    def CalcClusterARFCNConflictByCells(self,cluster):
        ConflictARFCN = {}
        for CGI in cluster:
            intfCGIs = list(cluster)
            intfCGIs.remove(CGI)
            for ICGI in intfCGIs:
                try:
                    for arfcn in set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[ICGI]['TRX'].keys()):
                        try:
                            ConflictARFCN[arfcn].add(CGI)
                            ConflictARFCN[arfcn].add(ICGI)
                        except KeyError:
                            ConflictARFCN[arfcn] = set()
                            ConflictARFCN[arfcn].add(CGI)
                            ConflictARFCN[arfcn].add(ICGI)
                except KeyError:
                    self.logger.warn('Cell {} and {} without TRX'.format(CGI,ICGI))
                    raise
        return  ConflictARFCN

    def CalcClusterARFCNConflictByClusterID(self,clusterId):
        ConflictARFCN = {}
        calcedPair = []
        if clusterId in self.hashedMaximalConnClusters:
            for CGI in self.hashedMaximalConnClusters[clusterId]:
                intfCGIs = list(self.hashedMaximalConnClusters[clusterId])
                intfCGIs.remove(CGI)
                for ICGI in intfCGIs:
                    if set((CGI,ICGI)) not in calcedPair:
                        try:
                            for arfcn in set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[ICGI]['TRX'].keys()):
                                try:
                                    ConflictARFCN[arfcn].add(CGI)
                                    ConflictARFCN[arfcn].add(ICGI)
                                except KeyError:
                                    ConflictARFCN[arfcn] = set()
                                    ConflictARFCN[arfcn].add(CGI)
                                    ConflictARFCN[arfcn].add(ICGI)
                        except KeyError:
                            self.logger.warn('Cell {} and {} without TRX'.format(CGI,ICGI))
                            raise
                        calcedPair.append(set((CGI,ICGI)))
        return  ConflictARFCN

#    def _CalcClusterARFCNConflict(self,cluster):
#        ConflictARFCN = {}
#        for CGI in cluster:
#            intfCGIs = list(cluster)
#            intfCGIs.remove(CGI)
#            for ICGI in intfCGIs:
#                try:
#                    for arfcn in set(self.GCELL[CGI]['TRX'].keys()).intersection(self.GCELL[ICGI]['TRX'].keys()):
#                        try:
#                            ConflictARFCN[arfcn].add(CGI)
#                            ConflictARFCN[arfcn].add(ICGI)
#                        except KeyError:
#                            ConflictARFCN[arfcn] = set()
#                            ConflictARFCN[arfcn].add(CGI)
#                            ConflictARFCN[arfcn].add(ICGI)
#
#                        try:
#                            self.GCELL[CGI]['CoArfcnCell'].add(ICGI)
#                        except KeyError:
#                            if CGI in self.GCELL:
#                                self.GCELL[CGI]['CoArfcnCell'] = set()
#                                self.GCELL[CGI]['CoArfcnCell'].add(ICGI)
#
#                        try:
#                            self.GCELL[ICGI]['CoArfcnCell'].add(CGI)
#                        except KeyError:
#                            if ICGI in self.GCELL:
#                                self.GCELL[ICGI]['CoArfcnCell'] = set()
#                                self.GCELL[ICGI]['CoArfcnCell'].add(CGI)
#
#                except KeyError:
#                    self.logger.warn('Cell {} and {} without TRX'.format(CGI,ICGI))
#                    raise
#        return  ConflictARFCN

    def GenSiteMap(self,filePath,zone = 50):
        self.logger.info('{} started....'.format('GenSiteMap'))
        #TODO FIXME Project system
        Proj = pyproj.Proj(proj='utm',zone=zone,ellps='WGS84')
        if filePath:
            self.logger.info('Writing Geo Site')
            with open('{}.mid'.format(filePath),'wb') as midfp:
                mid = csv.writer(midfp)
                with open('{}.mif'.format(filePath),'wb') as miffp:
                    miffp.write(
                        '''Version   450
                        Charset "WindowsSimpChinese"
                        Delimiter ","
                        CoordSys Earth Projection 1, 0
                        '''
                    )
                    header = ['CGI','HEX','DIR','NAME','BCCH','ARFCN']
                    miffp.write('Columns %s\r\n' % len(header))
                    for col in header:
                        if col in ['DIR',]:
                            miffp.write('  %s Float\r\n' % col)
                        elif col in ['BCCH',]:
                            miffp.write('  %s Integer\r\n' % col)
                        elif col in ['CGI','HEX','BAND',]:
                            miffp.write('  %s Char(50)\r\n' % col)
                        else:
                            miffp.write('  %s Char(254)\r\n' % col)
                    miffp.write('Data\r\n\r\n')

                    for CGI in self.CGI2GEO:
                        if CGI in self.CGICoverageBoundary:
                            BCCH = ''
                            endPoint = self.CGICoverageBoundary[CGI].centroid
                            HEXCGI = '{:X}{:X}'.format(int(CGI.split('-')[2]),int(CGI.split('-')[3]))
                            row = [CGI,HEXCGI]

                            dir  = self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['dir']
                            row.append(dir)

                            if CGI in self.GCELL:
                                #actually CGICoverageBoundar only generated from self.GCELL list
                                BCCH = self.GCELL[CGI].get('BCCH','')
                                row.append(self.GCELL[CGI].get('NAME','NoName'))
                                row.append(BCCH)
                                row.append(self.GCELL[CGI].get('TRX',{}).keys())
                            else:
                                row.append('NoName')
                                row.append(BCCH)
                                row.append([BCCH])

                            if self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] in ['indoor','underlayer']:
                                x1,y1 = self.GEOSets[self.CGI2GEO[CGI]]['coords']
                                x1 += random.randrange(-10,10)
                                y1 += random.randrange(-10,10)
                                x1,y1 = Proj(x1,y1,inverse=True)

                                self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['endPoint'] = (x1,y1)
                                miffp.write('Point {} {} \r\n'.format(x1,y1))
                                if int(BCCH.strip()) in range(1,125) or int(BCCH.strip()) in range(1000,1025):
                                    miffp.write('    Symbol (39,16711680,9)\r\n')
                                else:
                                    miffp.write('    Symbol (39,0,9)\r\n')
                                mid.writerow(row)
                            elif self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] == 'macro':
                                x1,y1 = self.GEOSets[self.CGI2GEO[CGI]]['coords']
                                x1,y1 = Proj(x1,y1,inverse=True)
                                x2,y2 = Proj(endPoint.x,endPoint.y,inverse=True)
                                self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['endPoint'] = (x2,y2)
                                miffp.write('Line {} {} {} {}\r\n'.format(x1,y1,x2,y2))
                                if int(BCCH.strip()) in range(1,125) or int(BCCH.strip()) in range(1000,1025):
                                    miffp.write('    Pen (20,79,16711680)\r\n')
                                else:
                                    miffp.write('    Pen (20,87,65535)\r\n')
                                mid.writerow(row)
                            else:
                                self.logger.warn('Unsupported cell type to draw site')
                        elif CGI in self.GCELL:
                            warn = 'CGI {} do not have coverage prediction,cell coverage directional demo escaped.'.format(CGI)
                            if warn not in self.EscapedWarn:
                                self.logger.warn(warn)
                                self.EscapedWarn.append(warn)

    def improved_write_mapped_MR(self,filePath):
        if filePath:
            self.logger.info('Output matched MR records.')
            #fp = open('{}.mapped.csv'.format(filePath),'wb')
            fp = open(filePath,'wb')
            writer = csv.writer(fp)
            writer.writerow(['CGI','SrvCell','SrvBCCH','BCCH#NCC#BCC','NCGI','NeiCell','Defined','S361','S369','S360','S360/S361','ServMRs/6','Distance'])
            cache = []
            count = 0
            maxCount = len(self.RAW_MR)
            for CGI in self.RAW_MR:
                for BB in self.RAW_MR[CGI]:
                    row = [CGI,self.GCELL.get(CGI,{'NAME':'NoneRecord'})['NAME'],self.GCELL.get(CGI,{'BCCH':'Unknown'})['BCCH'],BB,self.RAW_MR[CGI][BB]['NCGI'],self.GCELL.get(self.RAW_MR[CGI][BB]['NCGI'],{'NAME':'NoneRecord'})['NAME'],self.RAW_MR[CGI][BB]['Defined'],self.RAW_MR[CGI][BB]['S361'],self.RAW_MR[CGI][BB]['S369'],self.RAW_MR[CGI][BB]['S360'],self.RAW_MR[CGI][BB]['S360/S361'],self.ServCellMR[CGI]/6]
                    if CGI in self.CGI2GEO and self.RAW_MR[CGI][BB]['NCGI'] in self.CGI2GEO:
                        row.append(shapely.geometry.Point(self.GEOSets[self.CGI2GEO[CGI]]['coords']).distance(shapely.geometry.Point(self.GEOSets[self.CGI2GEO[self.RAW_MR[CGI][BB]['NCGI']]]['coords'])))
                    else:
                        row.append('Unknown')
                    cache.append(row)
                writer.writerows(cache)
                cache = []
                count += 1
                self.progressQueue.put(count*100/maxCount)
            fp.close()
            self.logger.info('Output metched MR records complete.')
            self.RAW_MR = []
            self.logger.info('Raw MR reports releaseed.')
        else:
            self.logger.error('Missing file path for MR report.')

    def write_mapped_MR(self,filePath):
        if filePath:
            self.logger.info('Output matched MR records.')
            #fp = open('{}.mapped.csv'.format(filePath),'wb')
            fp = open(filePath,'wb')
            writer = csv.writer(fp)
            writer.writerow(['CGI','SrvCell','SrvBCCH','BCCH#NCC#BCC','NCGI','NeiCell','Defined','S361','S369','S360','S360/S361','ServMRs/6','Distance'])
            for CGI in self.RAW_MR:
                for BB in self.RAW_MR[CGI]:
                    row = [CGI,self.GCELL.get(CGI,{'NAME':'NoneRecord'})['NAME'],self.GCELL.get(CGI,{'BCCH':'Unknown'})['BCCH'],BB,self.RAW_MR[CGI][BB]['NCGI'],self.GCELL.get(self.RAW_MR[CGI][BB]['NCGI'],{'NAME':'NoneRecord'})['NAME'],self.RAW_MR[CGI][BB]['Defined'],self.RAW_MR[CGI][BB]['S361'],self.RAW_MR[CGI][BB]['S369'],self.RAW_MR[CGI][BB]['S360'],self.RAW_MR[CGI][BB]['S360/S361'],self.ServCellMR[CGI]/6]
                    if CGI in self.CGI2GEO and self.RAW_MR[CGI][BB]['NCGI'] in self.CGI2GEO:
                        row.append(shapely.geometry.Point(self.GEOSets[self.CGI2GEO[CGI]]['coords']).distance(shapely.geometry.Point(self.GEOSets[self.CGI2GEO[self.RAW_MR[CGI][BB]['NCGI']]]['coords'])))
                    else:
                        row.append('Unknown')
                    writer.writerow(row)
            fp.close()
            self.logger.info('Output metched MR records complete.')
            self.RAW_MR = []
            self.logger.info('Raw MR reports releaseed.')

    def calcCellMrQual(self):
        self.logger.info('Start cell coverage interference status...')
        count= 0
        maxCount= len(self.CGI2HashedClusters)
        self.CalcDelauNeis()
        self.CalcARFCNConflictByNetwork()
        if not self.isMaximalConnectClusterDetected:
            self.CalcMaximalConnectCluster()
        #TODO FIXME change self.CGI2HashedClusters to self.GCELL,need test stablity
        #for CGI in self.CGI2HashedClusters:
        for CGI in self.GCELL:
            try:
                self.CellCoverageInfo[CGI] = {}
                #self.CellCoverageInfo[CGI]['CoFreqAffectingOthers'] = self.CalcCoArfcnAffectingOthers(CGI)
                #self.CellCoverageInfo[CGI]['CoFreqAffectedBy'] = self.CalcCoArfcnAffectedByOthers(CGI)
                self.CellCoverageInfo[CGI]['SrvCellInferiorTo12dBMRs'] = self.CalcServCellInferiortoNeisBy12dB(CGI)
                self.CellCoverageInfo[CGI]['SrvCellIntfToNeiMRsBy12dB'] = self.CalcServCellIntfoNeisBy12dB(CGI)
                self.CellCoverageInfo[CGI]['SrvCellOverlapedNeis'],self.CellCoverageInfo[CGI]['SrvCellOverlapedNeisBeyondDelauny'],self.CellCoverageInfo[CGI]['SrvCellOverlapedTRXs'],self.CellCoverageInfo[CGI]['SrvCellOverlapedTRXsBeyondDelNei'] = self.CalcServCellOverlappedNeisBy12dB(CGI)
                self.CellCoverageInfo[CGI]['SrvMRs'] = self.ServCellMR.get(CGI,-6)/6
                self.CellCoverageInfo[CGI]['TotalClusterCount'] = len(self.CGI2HashedClusters.get(CGI,[]))
                self.CellCoverageInfo[CGI]['CoArfcnCells'] = len(self.GCELL[CGI].get('CoArfcnCell',[]))
                if CGI in self.GCELL:
                    if int(self.GCELL[CGI].get('BCCH')) in range(512,886):
                        TRXThres = 80
                    else:
                        TRXThres = 60
                else:
                    TRXThres = 60
                self.CellCoverageInfo[CGI]['CriticalClusterCount'] = len([hashid for hashid in self.CGI2HashedClusters.get(CGI,[]) if sum([len(self.GCELL.get(SCGI,{'TRX':[]}).get('TRX',[])) for SCGI in self.hashedMaximalConnClusters[hashid]])> TRXThres])
            except:
                self.logger.exception('Unexpected error found when calc CGI {}'.format(CGI))
            finally:
                count+=1
                self.progressQueue.put(count*100/maxCount)
        self.isCellMrInfoCalced = True
        self.logger.info('Cell coverage interference status calculation completed.')

    def simplifiedClusterReport(self,filePath):
        self.ClusterAttribute = {} #初始化
        if not filePath:
            self.logger.info('Missing cluster report path.')
            return False
        self.logger.info('Simplified maximal connected cluster report output in progress...')
        with open(filePath,'wb') as fp:
        #with open('{}.cluter.csv'.format(filePath),'wb') as fp:
            writer = csv.writer(fp)
            writer.writerow(['CGI','小区名称','小区BCCH','小区载波数','服务小区测量报告数',
                             #'对其他小区的同频干扰次数','被其他小区同频干扰次数',
                             '同频小区数',
                             '服务小区电平相对强度不大于12dB本小区测量报告数','本小区在其他服务小区中存在电平竞争测量报告数','覆盖小区数','覆盖载波数','过覆盖小区数','过覆盖载波数','过覆盖小区',
                             '相关的最大连通簇','相关的问题最大连通簇',
                             '当前最大连通簇载波数','载波超限','当前最大连通簇载波数同频频点数','当前最大连通簇载波数同频载波数','当前最大连通簇载波数同频详情',
                             '当前最大连通簇小区数','当前最大连通簇测量报告数',
                             #'当前最大连通簇簇内同频干扰测量报告数','簇内同频干扰占比','服务小区在簇内同频干扰测量报告数',
                             #'当前最大连通簇簇内电平强度差不大于12dB的测量报告数',
                             '当前最大连通簇小区','连通簇ID'])
            if not self.isCellMrInfoCalced:
                self.calcCellMrQual()
            CGICount = 0
            MaxCount = len(self.CGI2HashedClusters)
            for CGI in self.CGI2HashedClusters:
                cache = []
                ServCell = self.GCELL[CGI]['NAME']
                SrvBCCH = self.GCELL[CGI]['BCCH']
                SrvTRXs = len(self.GCELL[CGI].get('TRX',[]))
                #CoFreqAffectingOthers = self.CalcCoArfcnAffectingOthers(CGI)
                #CoFreqAffectedBy = self.CalcCoArfcnAffectedByOthers(CGI)
                #SrvCellInferiorTo12dBMRs = self.CalcServCellInferiortoNeisBy12dB(CGI)
                #SrvCellIntfToNeiMRsBy12dB = self.CalcServCellIntfoNeisBy12dB(CGI)
                #SrvCellOverlapedNeis,SrvCellOverlapedNeisBeyondDelauny,SrvCellOverlapedTRXs,SrvCellOverlapedTRXsBeyondDelNei= self.CalcServCellOverlappedNeisBy12dB(CGI)
                #SrvMRs = self.ServCellMR.get(CGI,-6)/6
                #TotalClusterCount = len(self.CGI2HashedClusters[CGI])
                #CoArfcnCells = len(self.GCELL[CGI].get('CoArfcnCell',[]))
                if int(self.GCELL[CGI]['BCCH']) in range(512,886):
                    TRXThres = 80
                else:
                    TRXThres = 60
#                CriticalClusterCount = len([hashid for hashid in self.CGI2HashedClusters[CGI] if sum([len(self.GCELL.get(SCGI,{'TRX':[]}).get('TRX',[])) for SCGI in self.hashedMaximalConnClusters[hashid]])> TRXThres])
                #CoFreqAffectingOthers = self.CellCoverageInfo.get(CGI,{}).get('CoFreqAffectingOthers')
                #CoFreqAffectedBy = self.CellCoverageInfo.get(CGI,{}).get('CoFreqAffectedBy')
                SrvCellInferiorTo12dBMRs = self.CellCoverageInfo.get(CGI,{}).get('SrvCellInferiorTo12dBMRs')
                SrvCellIntfToNeiMRsBy12dB = self.CellCoverageInfo.get(CGI,{}).get('SrvCellIntfToNeiMRsBy12dB')
                SrvCellOverlapedNeis = self.CellCoverageInfo.get(CGI,{}).get('SrvCellOverlapedNeis')
                SrvCellOverlapedNeisBeyondDelauny = self.CellCoverageInfo.get(CGI,{}).get('SrvCellOverlapedNeisBeyondDelauny')
                SrvCellOverlapedTRXs = self.CellCoverageInfo.get(CGI,{}).get('SrvCellOverlapedTRXs')
                SrvCellOverlapedTRXsBeyondDelNei = self.CellCoverageInfo.get(CGI,{}).get('SrvCellOverlapedTRXsBeyondDelNei')
                SrvMRs = self.CellCoverageInfo.get(CGI,{}).get('SrvMRs')
                TotalClusterCount = self.CellCoverageInfo.get(CGI,{}).get('TotalClusterCount')
                CoArfcnCells = self.CellCoverageInfo.get(CGI,{}).get('CoArfcnCells')
                CriticalClusterCount = self.CellCoverageInfo.get(CGI,{}).get('CriticalClusterCount')
                for clusterid in self.CGI2HashedClusters[CGI]:
                    #clusterid = tuple(ref())
                    if clusterid not in self.ClusterAttribute:
                        CurrentClusterTRXs = 0
                        CurrentClusterScale = 0
                        CurrentClusterMRs = 0
                        #CurrentClusterIntfMRs = self.CalcClusterIntfMRs2(ref())
                        CurrentClusterConflictArfcn = self.CalcClusterARFCNConflictByClusterID(clusterid)
                        #intfInCluster = self.CalcCellIntfMRsInCluster(CGI,ref())
                        #CurrentClusterInferiorTo12dBMRs = self.CalcClusterInferiorMRsBy12dB(ref())
                        for SCGI in self.hashedMaximalConnClusters[clusterid]:
                            CurrentClusterTRXs += len(self.GCELL.get(SCGI,{'TRX':[]}).get('TRX',[]))
                            CurrentClusterScale+=1
                            try:
                                CurrentClusterMRs += self.ServCellMR[SCGI]/6
                            except KeyError:
                                pass
                        self.ClusterAttribute[clusterid] = {}
                        self.ClusterAttribute[clusterid]['CurrentClusterTRXs'] = CurrentClusterTRXs
                        self.ClusterAttribute[clusterid]['CurrentClusterScale'] = CurrentClusterScale
                        self.ClusterAttribute[clusterid]['CurrentClusterMRs'] = CurrentClusterMRs
                        #self.ClusterAttribute[clusterid]['CurrentClusterIntfMRs'] = CurrentClusterIntfMRs
                        self.ClusterAttribute[clusterid]['CurrentClusterConflictArfcn'] = CurrentClusterConflictArfcn
                        #self.ClusterAttribute[clusterid]['intfInCluster'] = intfInCluster
                        #self.ClusterAttribute[clusterid]['CurrentClusterInferiorTo12dBMRs'] = CurrentClusterInferiorTo12dBMRs
                    else:
                        CurrentClusterTRXs = self.ClusterAttribute[clusterid]['CurrentClusterTRXs']
                        CurrentClusterScale = self.ClusterAttribute[clusterid]['CurrentClusterScale']
                        CurrentClusterMRs = self.ClusterAttribute[clusterid]['CurrentClusterMRs']
                        #CurrentClusterIntfMRs = self.ClusterAttribute[clusterid]['CurrentClusterIntfMRs']
                        CurrentClusterConflictArfcn = self.ClusterAttribute[clusterid]['CurrentClusterConflictArfcn']
                        #intfInCluster = self.ClusterAttribute[clusterid]['intfInCluster']
                        #CurrentClusterInferiorTo12dBMRs = self.ClusterAttribute[clusterid]['CurrentClusterInferiorTo12dBMRs']
                    cache.append([CGI,ServCell, SrvBCCH,SrvTRXs,SrvMRs,
                                  #CoFreqAffectingOthers,CoFreqAffectedBy,
                                  CoArfcnCells,
                                  SrvCellInferiorTo12dBMRs,SrvCellIntfToNeiMRsBy12dB,len(SrvCellOverlapedNeis),SrvCellOverlapedTRXs,len(SrvCellOverlapedNeisBeyondDelauny),SrvCellOverlapedTRXsBeyondDelNei,SrvCellOverlapedNeisBeyondDelauny,
                                  TotalClusterCount,CriticalClusterCount,
                                  CurrentClusterTRXs,
                                  CurrentClusterTRXs > TRXThres,
                                  len(CurrentClusterConflictArfcn),
                                  sum([len(CurrentClusterConflictArfcn[arfcn]) for arfcn in CurrentClusterConflictArfcn]),
                                  CurrentClusterConflictArfcn,
                                  CurrentClusterScale,
                                  CurrentClusterMRs,
                                  #CurrentClusterIntfMRs,
                                  #'{:.3%}'.format(CurrentClusterIntfMRs/CurrentClusterMRs),intfInCluster,
                                  #CurrentClusterInferiorTo12dBMRs,
                                  self.hashedMaximalConnClusters[clusterid],
                                  clusterid
                    ])
                writer.writerows(cache)
                CGICount+=1
                self.progressQueue.put(CGICount*100/MaxCount)
            self.logger.info('Simplified maximal cluster report output done.')

    def _output_cluterReport(self,filePath):
        #For correlateDetect3 only
        self.ClusterAttribute = {} #初始化
        CGICount = 0
        if not filePath:
            self.logger.info('Missing cluster report path.')
            return False
        self.logger.info('Cluster report output in progress...')
        with open(filePath,'wb') as fp:
        #with open('{}.cluter.csv'.format(filePath),'wb') as fp:
            writer = csv.writer(fp)
            writer.writerow(['CGI','小区名称','小区BCCH','小区载波数','服务小区测量报告数',
                             '对其他小区的同频干扰次数','被其他小区同频干扰次数','同频小区数',
                             '服务小区电平相对强度不大于12dB本小区测量报告数','本小区在其他服务小区中存在电平竞争测量报告数','覆盖小区数','覆盖载波数','过覆盖小区数','过覆盖载波数','过覆盖小区',
                             '相关的最大连通簇','相关的问题最大连通簇',
                             '当前最大连通簇载波数','载波超限','当前最大连通簇载波数同频频点数','当前最大连通簇载波数同频载波数','当前最大连通簇载波数同频详情',
                             '当前最大连通簇小区数','当前最大连通簇测量报告数','当前最大连通簇簇内同频干扰测量报告数','簇内同频干扰占比','服务小区在簇内同频干扰测量报告数',
                             '当前最大连通簇簇内电平强度差不大于12dB的测量报告数',
                             '当前最大连通簇小区'])
            for CGI in self.CGI2Clusters:
                ServCell = self.GCELL[CGI]['NAME']
                SrvBCCH = self.GCELL[CGI]['BCCH']
                SrvTRXs = len(self.GCELL[CGI].get('TRX',[]))
                CoFreqAffectingOthers = self.CalcCoArfcnAffectingOthers(CGI)
                CoFreqAffectedBy = self.CalcCoArfcnAffectedByOthers(CGI)
                SrvCellInferiorTo12dBMRs = self.CalcServCellInferiortoNeisBy12dB(CGI)
                SrvCellIntfToNeiMRsBy12dB = self.CalcServCellIntfoNeisBy12dB(CGI)
                SrvCellOverlapedNeis,SrvCellOverlapedNeisBeyondDelauny,SrvCellOverlapedTRXs,SrvCellOverlapedTRXsBeyondDelNei= self.CalcServCellOverlappedNeisBy12dB(CGI)
                SrvMRs = self.ServCellMR.get(CGI,-6)/6
                TotalClusterCount = len(self.CGI2Clusters[CGI])
                CoArfcnCells = len(self.GCELL[CGI].get('CoArfcnCell',[]))
                if int(self.GCELL[CGI]['BCCH']) in range(512,886):
                    TRXThres = 80
                else:
                    TRXThres = 60
                CriticalClusterCount = len([ref for ref in self.CGI2Clusters[CGI] if sum([len(self.GCELL.get(SCGI,{'TRX':[]}).get('TRX',[])) for SCGI in ref()])> TRXThres])
                for ref in self.CGI2Clusters[CGI]:
                    clusterid = tuple(ref())
                    if clusterid not in self.ClusterAttribute:
                        CurrentClusterTRXs = 0
                        CurrentClusterScale = 0
                        CurrentClusterMRs = 0
                        CurrentClusterIntfMRs = self.CalcClusterIntfMRs2(ref())
                        CurrentClusterConflictArfcn = self.CalcClusterARFCNConflictByCells(ref())
                        intfInCluster = self.CalcCellIntfMRsInCluster(CGI,ref())
                        CurrentClusterInferiorTo12dBMRs = self.CalcClusterInferiorMRsBy12dB(ref())
                        for SCGI in ref():
                            CurrentClusterTRXs += len(self.GCELL.get(SCGI,{'TRX':[]}).get('TRX',[]))
                            CurrentClusterScale+=1
                            try:
                                CurrentClusterMRs += self.ServCellMR[SCGI]/6
                            except KeyError:
                                pass

                        self.ClusterAttribute[clusterid] = {}
                        self.ClusterAttribute[clusterid]['CurrentClusterTRXs'] = CurrentClusterTRXs
                        self.ClusterAttribute[clusterid]['CurrentClusterScale'] = CurrentClusterScale
                        self.ClusterAttribute[clusterid]['CurrentClusterMRs'] = CurrentClusterMRs
                        self.ClusterAttribute[clusterid]['CurrentClusterIntfMRs'] = CurrentClusterIntfMRs
                        self.ClusterAttribute[clusterid]['CurrentClusterConflictArfcn'] = CurrentClusterConflictArfcn
                        self.ClusterAttribute[clusterid]['intfInCluster'] = intfInCluster
                        self.ClusterAttribute[clusterid]['CurrentClusterInferiorTo12dBMRs'] = CurrentClusterInferiorTo12dBMRs
                    else:
                        CurrentClusterTRXs = self.ClusterAttribute[clusterid]['CurrentClusterTRXs']
                        CurrentClusterScale = self.ClusterAttribute[clusterid]['CurrentClusterScale']
                        CurrentClusterMRs = self.ClusterAttribute[clusterid]['CurrentClusterMRs']
                        CurrentClusterIntfMRs = self.ClusterAttribute[clusterid]['CurrentClusterIntfMRs']
                        CurrentClusterConflictArfcn = self.ClusterAttribute[clusterid]['CurrentClusterConflictArfcn']
                        intfInCluster = self.ClusterAttribute[clusterid]['intfInCluster']
                        CurrentClusterInferiorTo12dBMRs = self.ClusterAttribute[clusterid]['CurrentClusterInferiorTo12dBMRs']

                    writer.writerow([CGI,ServCell, SrvBCCH,SrvTRXs,SrvMRs,
                                     CoFreqAffectingOthers,CoFreqAffectedBy,CoArfcnCells,
                                     SrvCellInferiorTo12dBMRs,SrvCellIntfToNeiMRsBy12dB,len(SrvCellOverlapedNeis),SrvCellOverlapedTRXs,len(SrvCellOverlapedNeisBeyondDelauny),SrvCellOverlapedTRXsBeyondDelNei,SrvCellOverlapedNeisBeyondDelauny,
                                     TotalClusterCount,CriticalClusterCount,
                                     CurrentClusterTRXs,
                                     CurrentClusterTRXs > TRXThres,
                                     len(CurrentClusterConflictArfcn),
                                     sum([len(CurrentClusterConflictArfcn[arfcn]) for arfcn in CurrentClusterConflictArfcn]),
                                     CurrentClusterConflictArfcn,
                                     CurrentClusterScale,
                                     CurrentClusterMRs,
                                     CurrentClusterIntfMRs,
                                     '{:.3%}'.format(CurrentClusterIntfMRs/CurrentClusterMRs),intfInCluster,
                                     CurrentClusterInferiorTo12dBMRs,
                                     list(ref())])
                CGICount+=1
                if CGICount*100/len(self.CGI2Clusters) % 10 == 0 and (CGICount-1)*100/len(self.CGI2Clusters) % 10 != 0:
                    #print('Outputing processed cell {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.CGI2Clusters)))
                    self.progressQueue.put(CGICount*100/len(self.CGI2Clusters))
            self.logger.info('Cluster report output done.')
                    #self.logger.info('Outputing processed cell {} {:.2%}.'.format(CGICount,CGICount*1.0/len(self.CGI2Clusters)))

    def CalcGridAttributes(self,grid):
        #print('Start to calc grid attributes...')

        def _CalcGridAttribute(grid,attr,escapedCGIs):
            self.GridAttributes[grid]['{}_900M'.format(attr)] = -0.0000001
            self.GridAttributes[grid]['{}_1800M'.format(attr)] = -0.0000001
            self.GridAttributes[grid]['{}'.format(attr)] = -0.0000001
            escapeingCGIs = []
            escaped900MErl = 0
            escaped1800MErl = 0
            for cgi in escapedCGIs:
                if cgi in self.GCELL:
                    BCCH = self.GCELL[cgi].get('BCCH')
                    if int(BCCH.strip()) in range(1,125) or int(BCCH.strip()) in range(1000,1025):
                        escaped900MErl += self.GridMatrix[grid][cgi]['ERL']
                        assert escaped900MErl <= self.GridAttributes[grid]['Traffic900']
                    elif int(BCCH.strip()) in range(512,849) :
                        escaped1800MErl += self.GridMatrix[grid][cgi]['ERL']
                        assert escaped1800MErl <= self.GridAttributes[grid]['Traffic1800']
                    else:
                        self.logger.warn('CGI {} do not have valid BCCH,both 900/1800 escaping ERL.'.format(cgi))
                        escaped900MErl += self.GridMatrix[grid][cgi]['ERL']
                        escaped1800MErl += self.GridMatrix[grid][cgi]['ERL']
                else:
                    self.logger.warn('CGI {} do not in GCELL,both 900/1800 escaping ERL.'.format(cgi))
                    escaped900MErl += self.GridMatrix[grid][cgi]['ERL']
                    escaped1800MErl += self.GridMatrix[grid][cgi]['ERL']

            for CGI in self.GridAttributes[grid]['CGIs']:
                if CGI in escapedCGIs:
                    #不处理之前存在异常的小区
                    continue
                if CGI in self.CGIPerformance and CGI in self.GCELL:
                    BCCH = self.GCELL[CGI].get('BCCH')
                    if BCCH:
                        try:
                            if int(BCCH.strip()) in range(1,125) or int(BCCH.strip()) in range(1000,1025):
                                self.GridAttributes[grid]['{}_900M'.format(attr)] += self.CGIPerformance[CGI][attr] * self.GridMatrix[grid][CGI]['ERL'] /( self.GridAttributes[grid]['Traffic900'] - escaped900MErl)
                            elif int(BCCH.strip()) in range(512,849) :
                                self.GridAttributes[grid]['{}_1800M'.format(attr)] += self.CGIPerformance[CGI][attr] * self.GridMatrix[grid][CGI]['ERL'] / ( self.GridAttributes[grid]['Traffic1800'] - escaped1800MErl)
                            else:
                                self.logger.warn('CGI {} do not have valid BCCH,mapping in grid will be escaped.'.format(CGI))
                                if CGI not in escapeingCGIs:
                                    escapeingCGIs.append(CGI)
                        except ZeroDivisionError:
                            pass
                        except KeyError:
                            if attr not in  self.CGIPerformance[CGI]:
                                warn = 'CGI {} do not have {} data'.format(CGI,attr)
                                if warn not in self.EscapedWarn:
                                    self.logger.warn(warn)
                                    #print(warn)
                                    self.EscapedWarn.append(warn)
                                if CGI not in escapeingCGIs:
                                    escapeingCGIs.append(CGI)
                            else :
                                raise Exception
                    else:
                        self.logger.warn('CGI {} do not have valid BCCH,mapping in grid will be escaped.'.format(CGI))
                        if CGI not in escapeingCGIs:
                            escapeingCGIs.append(CGI)

                    try:
                        self.GridAttributes[grid]['{}'.format(attr)] += self.CGIPerformance[CGI][attr] * self.GridMatrix[grid][CGI]['ERL'] / ( self.GridAttributes[grid]['TrafficTotal'] - sum([self.GridMatrix[grid][cgi]['ERL'] for cgi in escapedCGIs]))
                        assert  self.GridAttributes[grid]['TrafficTotal'] >= sum([self.GridMatrix[grid][cgi]['ERL'] for cgi in escapedCGIs])
                    except ZeroDivisionError:
                        pass
                    except KeyError:
                        if attr not in  self.CGIPerformance[CGI]:
                            warn = 'CGI {} do not have {} data'.format(CGI,attr)
                            if warn not in self.EscapedWarn:
                                self.logger.warn(warn)
                                #print(warn)
                                self.EscapedWarn.append(warn)
                            if CGI not in escapeingCGIs:
                                escapeingCGIs.append(CGI)
                        else :
                            raise Exception
                else:
                    if CGI not in escapeingCGIs:
                        escapeingCGIs.append(CGI)
                    warn ='CGI {} do not have performace data or not in gcell configuration,calc will be escaped.'.format(CGI)
                    if warn not in self.EscapedWarn:
                        #print(warn)
                        self.logger.warn(warn)
                        self.EscapedWarn.append(warn)

            if escapeingCGIs:
                #print('Recalc grid {} attribute {},escaping {}'.format(grid,attr,escapeingCGIs+escapedCGIs))
                _CalcGridAttribute(grid,attr,escapeingCGIs+escapedCGIs)

        if grid in self.GridMatrix:
            self.GridAttributes [grid] = {}
            CGIs = self.GridMatrix[grid].items()
            CGIs.sort(key = lambda x:x[1]['ERL'],reverse = True)
            self.GridAttributes[grid]['CGIs'] = [item[0] for item in CGIs]
            self.GridAttributes[grid]['TrafficTotal'] = 0
            self.GridAttributes[grid]['TrafficUnderLayer'] = 0
            self.GridAttributes[grid]['TrafficMacro'] = 0
            self.GridAttributes[grid]['Traffic900Macro'] = 0
            self.GridAttributes[grid]['Traffic900'] = 0
            self.GridAttributes[grid]['Traffic1800Macro'] = 0
            self.GridAttributes[grid]['Traffic1800'] = 0
            self.GridAttributes[grid]['GridAvgCellDist'] = 0
            self.GridAttributes[grid]['TRXs_1800'] = 0
            self.GridAttributes[grid]['TRXs_900'] = 0
            self.GridAttributes[grid]['CellCount_1800'] = 0
            self.GridAttributes[grid]['CellCount_900'] = 0
            ncellCount = 0
            for CGI in self.GridAttributes[grid]['CGIs']:
                self.GridAttributes[grid]['TrafficTotal'] += self.GridMatrix[grid][CGI]['ERL']
                if CGI in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] in ['indoor','underlayer']:
                    self.GridAttributes[grid]['TrafficUnderLayer'] += self.GridMatrix[grid][CGI]['ERL']
                else:
                    self.GridAttributes[grid]['TrafficMacro'] += self.GridMatrix[grid][CGI]['ERL']
                if CGI in self.GCELL:
                    BCCH = self.GCELL[CGI].get('BCCH')
                    if BCCH:
                        if int(BCCH.strip()) in range(1,125) or int(BCCH.strip()) in range(1000,1025):
                            if CGI in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] not in ['indoor','underlayer']:
                                self.GridAttributes[grid]['Traffic900Macro'] += self.GridMatrix[grid][CGI]['ERL']
                            self.GridAttributes[grid]['Traffic900'] += self.GridMatrix[grid][CGI]['ERL']
                            self.GridAttributes[grid]['TRXs_900'] += len(self.GCELL[CGI].get('TRX',{}))
                            self.GridAttributes[grid]['CellCount_900'] += 1
                        elif int(BCCH.strip()) in range(512,849) :
                            if CGI in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] not in ['indoor','underlayer']:
                                self.GridAttributes[grid]['Traffic1800Macro'] += self.GridMatrix[grid][CGI]['ERL']
                            self.GridAttributes[grid]['Traffic1800'] += self.GridMatrix[grid][CGI]['ERL']
                            self.GridAttributes[grid]['TRXs_1800'] += len(self.GCELL[CGI].get('TRX',{}))
                            self.GridAttributes[grid]['CellCount_1800'] += 1
                    else:
                        self.logger.warn('CGI {} do not have valid BCCH,mapping in grid will be escaped.'.format(CGI))

                try:
                    self.GridAttributes[grid]['GridAvgCellDist'] += self.GCELL[CGI]['ncell_dist_avg']
                    ncellCount+= 1
                except :
                    pass
            try:
                self.GridAttributes[grid]['GridAvgCellDist'] /= ncellCount
            except ZeroDivisionError:
                self.GridAttributes[grid]['GridAvgCellDist'] = -1

            densityFactor = 1000000.0/self.GRID_SIZE/self.GRID_SIZE
            self.GridAttributes[grid]['TrafficDensityTotal'] = densityFactor*self.GridAttributes[grid]['TrafficTotal']
            self.GridAttributes[grid]['TrafficDensityMacro'] = densityFactor*self.GridAttributes[grid]['TrafficMacro']
            self.GridAttributes[grid]['TrafficDensity900Macro'] = densityFactor*self.GridAttributes[grid]['Traffic900Macro']
            self.GridAttributes[grid]['TrafficDensity1800Macro'] = densityFactor*self.GridAttributes[grid]['Traffic1800Macro']
            DistMin = max([m for m in self.MAX_ERLperKm2[self.MAX_BAND_WIDTH] if m <= self.GridAttributes[grid]['GridAvgCellDist'] ] + [300,])
            self.GridAttributes[grid]['AvgDistMinUpThreshold'] = DistMin
            self.GridAttributes[grid]['MacroExceeds'] = self.GridAttributes[grid]['TrafficDensityMacro'] - self.MAX_ERLperKm2[self.MAX_BAND_WIDTH][DistMin] - self.MAX_ERLperKm2[20][DistMin] #Total
            self.GridAttributes[grid]['Macro900MExceeds'] = self.GridAttributes[grid]['TrafficDensity900Macro'] - self.MAX_ERLperKm2[20][DistMin] #900M
            self.GridAttributes[grid]['Macro1800MExceeds'] = self.GridAttributes[grid]['TrafficDensity1800Macro'] - self.MAX_ERLperKm2[self.MAX_BAND_WIDTH][DistMin] #1800M

            self.GridAttributes[grid]['LLCBytes'] = 0
            for CGI in self.GridAttributes[grid]['CGIs']:
                if CGI in self.CGITraffic_TA:
                    try:
                        self.GridAttributes[grid]['LLCBytes'] += (self.CGITraffic_TA[CGI].get('L9506',0)+self.CGITraffic_TA[CGI].get('L9403',0))* self.GridMatrix[grid][CGI]['ERL'] / ( self.CGITraffic_TA[CGI]['K3014'] + self.CGITraffic_TA[CGI]['AR9311'])
                    except ZeroDivisionError:
                        pass
                else:
                    warn = 'CGI {} do not have TA MR information and ERL/PDCH data,grid data service attribute will be escaped.'.format(CGI)
                    if warn not in self.EscapedWarn:
                        #print(warn)
                        self.logger.warn(warn)
                        self.EscapedWarn.append(warn)


            for key in self.CGIPerformanceKey:
                _CalcGridAttribute(grid,key,[])

        return True

    def OutputGridToMIF_new(self,filePath,zone = 50):
        self.logger.info('{} started....'.format(__name__))
        #TODO FIXME Project system
        Proj = pyproj.Proj(proj='utm',zone=zone,ellps='WGS84')
        MaxCellsInGrid = 28
        if filePath:
            self.logger.info('Start output new gird mif to {}.mid..'.format(filePath))
            with open('{}.new.mid'.format(filePath),'wb') as midfp:
                mid = csv.writer(midfp)
                with open('{}.new.mif'.format(filePath),'wb') as miffp:
                    miffp.write('Version   450\r\n')
                    miffp.write('Charset "WindowsSimpChinese"\r\n')
                    miffp.write('Delimiter ","\r\n')
                    miffp.write('CoordSys Earth Projection 1, 0\r\n')
                    headerFixed = ['TRXs_900','TRXs_1800','CellCount_900','CellCount_1800','TrafficTotal','TrafficUnderLayer','TrafficDensityTotal','TrafficDensityMacro','TrafficDensity900Macro','TrafficDensity1800Macro','CellCount','CGIs','GridAvgCellDist','MacroExceeds','Macro900MExceeds','Macro1800MExceeds','LLCBytes'] + self.CGIPerformanceKey + ['{}_900M'.format(key) for key in self.CGIPerformanceKey] + ['{}_1800M'.format(key) for key in self.CGIPerformanceKey]
                    header = headerFixed + ['Cell_ERL_{}'.format(i) for i in xrange(0,MaxCellsInGrid)]
                    miffp.write('Columns %s\r\n' % len(header))
                    for col in header:
                        if col in ['TrafficTotal','TrafficUnderLayer','TrafficDensityTotal','TrafficDensityMacro','TrafficDensity900Macro','TrafficDensity1800Macro','GridAvgCellDist','MacroExceeds','Macro900MExceeds','Macro1800MExceeds','LLCBytes'] + self.CGIPerformanceKey + ['{}_900M'.format(key) for key in self.CGIPerformanceKey] + ['{}_1800M'.format(key) for key in self.CGIPerformanceKey]:
                            miffp.write('  %s Float\r\n' % col)
                        elif col in ['CellCount','TRXs_900','TRXs_1800','CellCount_900','CellCount_1800']:
                            miffp.write('  %s Integer\r\n' % col)
                        elif col in ['Cell_ERL_{}'.format(i) for i in xrange(0,MaxCellsInGrid)]:
                            miffp.write('  %s Char(50)\r\n' % col)
                        else:
                            miffp.write('  %s Char(254)\r\n' % col)

                    miffp.write('Data\r\n\r\n')
                    for grid in self.GridMatrix:
                        self.CalcGridAttributes(grid)
                        row = len(headerFixed) * [0,]
                        row[header.index('CGIs')] = ''

                        for CGI in self.GridAttributes[grid]['CGIs']:
                            HEXCGI = '{:X}{:X}:'.format(int(CGI.split('-')[2]),int(CGI.split('-')[3]))
                            row[header.index('CellCount')] += 1
                            if row[header.index('CellCount')] < MaxCellsInGrid:
                                row[header.index('CGIs')] += HEXCGI
                                row.append('{}{:.5f}'.format(HEXCGI,self.GridMatrix[grid][CGI]['ERL']))

                        row[header.index('TRXs_900')] = self.GridAttributes[grid]['TRXs_900']
                        row[header.index('TRXs_1800')] = self.GridAttributes[grid]['TRXs_1800']
                        row[header.index('CellCount_900')] = self.GridAttributes[grid]['CellCount_900']
                        row[header.index('CellCount_1800')] = self.GridAttributes[grid]['CellCount_1800']
                        row[header.index('TrafficTotal')] = self.GridAttributes[grid]['TrafficTotal']
                        row[header.index('TrafficUnderLayer')] = self.GridAttributes[grid]['TrafficUnderLayer']
                        row[header.index('TrafficDensityTotal')] = self.GridAttributes[grid]['TrafficDensityTotal']
                        row[header.index('TrafficDensityMacro')] = self.GridAttributes[grid]['TrafficDensityMacro']
                        row[header.index('TrafficDensity900Macro')] = self.GridAttributes[grid]['TrafficDensity900Macro']
                        row[header.index('TrafficDensity1800Macro')] = self.GridAttributes[grid]['TrafficDensity1800Macro']
                        row[header.index('MacroExceeds')] = self.GridAttributes[grid]['MacroExceeds']
                        row[header.index('Macro900MExceeds')] = self.GridAttributes[grid]['Macro900MExceeds']
                        row[header.index('Macro1800MExceeds')] = self.GridAttributes[grid]['Macro1800MExceeds']
                        row[header.index('GridAvgCellDist')] = self.GridAttributes[grid]['GridAvgCellDist']
                        row[header.index('LLCBytes')] = self.GridAttributes[grid]['LLCBytes']

                        for key in self.CGIPerformanceKey:
                            row[header.index(key)] = self.GridAttributes[grid][key]
                            row[header.index('{}_900M'.format(key))] = self.GridAttributes[grid]['{}_900M'.format(key)]
                            row[header.index('{}_1800M'.format(key))] = self.GridAttributes[grid]['{}_1800M'.format(key)]


                        if len(row) < len(header):
                            row.extend(['Null' for i in xrange(0,len(header)- len(row))])

                        if row[header.index('CellCount')] > 0:
                            #仅输出那些有实际覆盖的区域
                            mid.writerow(row)
                            miffp.write('Region  1\r\n')
                            miffp.write('  4\r\n')
                            x,y = Proj(grid[0] - self.GRID_SIZE/2,grid[1] - self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            x,y = Proj(grid[0] + self.GRID_SIZE/2,grid[1] - self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            x,y = Proj(grid[0] + self.GRID_SIZE/2,grid[1] + self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            x,y = Proj(grid[0] - self.GRID_SIZE/2,grid[1] + self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            miffp.write('    Pen (0,1,0)\r\n')
                            miffp.write('    Brush (1,16711680,16711680)\r\n')
                            x,y = Proj(grid[0],grid[1],inverse=True)
                            miffp.write('    Center %s %s\r\n' % (x,y))
                        del self.GridAttributes[grid]
            self.logger.info('Output done.')

    def _OutputGridtoMIF(self,filePath,zone = 50):
        #TODO FIXME Project system
        Proj = pyproj.Proj(proj='utm',zone=zone,ellps='WGS84')
        MaxCellsInGrid = 28
        if filePath:
            self.logger.info('Start output gird mif to {}.mid..'.format(filePath))
            with open('{}.mid'.format(filePath),'wb') as midfp:
                mid = csv.writer(midfp)
                with open('{}.mif'.format(filePath),'wb') as miffp:
                    miffp.write('Version   450\r\n')
                    miffp.write('Charset "WindowsSimpChinese"\r\n')
                    miffp.write('Delimiter ","\r\n')
                    miffp.write('CoordSys Earth Projection 1, 0\r\n')
                    headerFixed = ['TrafficTotal','TrafficUnderLayer','TrafficDensityTotal','TrafficDensityMacro','TrafficDensity900Macro','TrafficDensity1800Macro','CellCount','CGIs','GridAvgCellDist','MacroExceeds','Macro900MExceeds','Macro1800MExceeds']
                    header = headerFixed + ['Cell_ERL_{}'.format(i) for i in xrange(0,MaxCellsInGrid)]
                    miffp.write('Columns %s\r\n' % len(header))
                    for col in header:
                        if col in ['TrafficTotal','TrafficUnderLayer','TrafficDensityTotal','TrafficDensityMacro','TrafficDensity900Macro','TrafficDensity1800Macro','GridAvgCellDist','MacroExceeds','Macro900MExceeds','Macro1800MExceeds']:
                            miffp.write('  %s Float\r\n' % col)
                        elif col in ['CellCount',]:
                            miffp.write('  %s Integer\r\n' % col)
                        elif col in ['Cell_ERL_{}'.format(i) for i in xrange(0,MaxCellsInGrid)]:
                            miffp.write('  %s Char(50)\r\n' % col)
                        else:
                            miffp.write('  %s Char(254)\r\n' % col)

                    miffp.write('Data\r\n\r\n')
                    for grid in self.GridMatrix:
                        #row = [0,0,'',0,0,0,0,'900exceeds','1800exceeds']
                        row = len(headerFixed) * [0,]
                        row[header.index('CGIs')] = ''
                        #=================================
                        #                    if grid in self.GridSiteDist:
                        #                        row[3] = self.GridSiteDist[grid]
                        #                    else:
                        #                        row[3] = -1
                        #=================================
                        CGIs = self.GridMatrix[grid].items()
                        CGIs.sort(key = lambda x:x[1]['ERL'],reverse = True)

                        ncellCount = 0
                        for CGI in [item[0] for item in CGIs]:
                            HEXCGI = '{:X}{:X}:'.format(int(CGI.split('-')[2]),int(CGI.split('-')[3]))
                            row[header.index('TrafficTotal')] += self.GridMatrix[grid][CGI]['ERL']
                            #row[header.index('TrafficDensityTotal')] += self.GridMatrix[grid][CGI]['ERL']
                            if CGI in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] in ['indoor','underlayer']:
                                row[header.index('TrafficUnderLayer')] += self.GridMatrix[grid][CGI]['ERL']
                            else:
                                row[header.index('TrafficDensityMacro')] += self.GridMatrix[grid][CGI]['ERL']
                            if CGI in self.GCELL:
                                BCCH = self.GCELL[CGI].get('BCCH')
                                if BCCH:
                                    if int(BCCH.strip()) in range(1,125) or int(BCCH.strip()) in range(1000,1025):
                                        if CGI in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] not in ['indoor','underlayer']:
                                            row[header.index('TrafficDensity900Macro')] += self.GridMatrix[grid][CGI]['ERL']
                                        else:
                                            pass
                                    elif int(BCCH.strip()) in range(512,849) :
                                        if CGI in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI]['type'] not in ['indoor','underlayer']:
                                            row[header.index('TrafficDensity1800Macro')] += self.GridMatrix[grid][CGI]['ERL']
                                        else:
                                            pass

                            row[header.index('CellCount')] += 1
                            if row[header.index('CellCount')] < MaxCellsInGrid:
                                row[header.index('CGIs')] += HEXCGI
                                row.append('{}{:.5f}'.format(HEXCGI,self.GridMatrix[grid][CGI]['ERL']))

                            try:
                                row[header.index('GridAvgCellDist')] += self.GCELL[CGI]['ncell_dist_avg']
                                ncellCount+= 1
                            except :
                                pass

                        try:
                            row[header.index('GridAvgCellDist')] /= ncellCount
                        except ZeroDivisionError:
                            row[header.index('GridAvgCellDist')] = -1

                        densityFactor = 1000000.0/self.GRID_SIZE/self.GRID_SIZE
                        row[header.index('TrafficDensityTotal')] = densityFactor*row[header.index('TrafficTotal')]
                        row[header.index('TrafficDensityMacro')] *= densityFactor
                        row[header.index('TrafficDensity900Macro')] *= densityFactor
                        row[header.index('TrafficDensity1800Macro')] *= densityFactor
                        DistMin = max([m for m in self.MAX_ERLperKm2[self.MAX_BAND_WIDTH] if m <= row[header.index('GridAvgCellDist')] ] + [300,])
                        row[header.index('MacroExceeds')] = row[header.index('TrafficDensityMacro')] - self.MAX_ERLperKm2[self.MAX_BAND_WIDTH][DistMin] - self.MAX_ERLperKm2[20][DistMin] #Total
                        row[header.index('Macro900MExceeds')] = row[header.index('TrafficDensity900Macro')] - self.MAX_ERLperKm2[20][DistMin] #900M
                        row[header.index('Macro1800MExceeds')] = row[header.index('TrafficDensity1800Macro')] - self.MAX_ERLperKm2[self.MAX_BAND_WIDTH][DistMin] #1800M
                        #                    if row[0] > 0.001 and row[1] == 1 and '0.000' in row[3]:
                        #                        raise ArithmeticError
                        #Convert ERL to ERL/Km2
                        #                    row[0] = '{:.3f}'.format(row[0])
                        #                    row[5] = '{:.3f}'.format(row[5])
                        #                    row[6] = '{:.3f}'.format(row[6])
                        if len(row) < len(header):
                            row.extend(['Null' for i in xrange(0,len(header)- len(row))])
                        if row[header.index('CellCount')] > 0:
                            #仅输出那些有实际覆盖的区域
                            mid.writerow(row)
                            miffp.write('Region  1\r\n')
                            miffp.write('  4\r\n')
                            x,y = Proj(grid[0] - self.GRID_SIZE/2,grid[1] - self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            x,y = Proj(grid[0] + self.GRID_SIZE/2,grid[1] - self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            x,y = Proj(grid[0] + self.GRID_SIZE/2,grid[1] + self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            x,y = Proj(grid[0] - self.GRID_SIZE/2,grid[1] + self.GRID_SIZE/2,inverse=True)
                            miffp.write('%s %s\r\n' % (x,y))
                            miffp.write('    Pen (0,1,0)\r\n')
                            miffp.write('    Brush (1,16711680,16711680)\r\n')
                            x,y = Proj(grid[0],grid[1],inverse=True)
                            miffp.write('    Center %s %s\r\n' % (x,y))

            self.logger.info('Output done.')

    def DrawConflictPairs(self,filePath):
        if filePath:
            self.logger.info('Start output conflict pair to {}.conflict.mid..'.format(filePath))
            with open('{}.conflict.mid'.format(filePath),'wb') as midfp:
                mid = csv.writer(midfp)
                with open('{}.conflict.mif'.format(filePath),'wb') as miffp:
                    miffp.write('Version   450\r\n')
                    miffp.write('Charset "WindowsSimpChinese"\r\n')
                    miffp.write('Delimiter ","\r\n')
                    miffp.write('CoordSys Earth Projection 1, 0\r\n')
                    #headerFixed = ['CGI1','CGI2','CoARFCN','AdjARFCN']
                    header = ['CGI1','CGI2','CoARFCN','AdjARFCN','BAND']
                    miffp.write('Columns %s\r\n' % len(header))
                    for col in header:
                        if col in []:
                            miffp.write('  %s Float\r\n' % col)
                        elif col in []:
                            miffp.write('  %s Integer\r\n' % col)
                        elif col in ['CGI1','CGI2']:
                            miffp.write('  %s Char(50)\r\n' % col)
                        else:
                            miffp.write('  %s Char(254)\r\n' % col)

                    miffp.write('Data\r\n\r\n')
                    cellPair = {}
                    for clusterid in self.ClusterAttribute:
                        for arfcn in self.ClusterAttribute[clusterid]['CurrentClusterConflictArfcn']:
                            for CGI in self.ClusterAttribute[clusterid]['CurrentClusterConflictArfcn'][arfcn]:
                                CGIS =list(self.self.ClusterAttribute[clusterid]['CurrentClusterConflictArfcn'][arfcn])
                                CGIS.remove(CGI)
                                for ICGI in CGIS:
                                    try:
                                        cellPair[frozenset([CGI,ICGI])].add(arfcn)
                                    except KeyError:
                                        cellPair[frozenset([CGI,ICGI])] = set()
                                        cellPair[frozenset([CGI,ICGI])].add(arfcn)

                    for pair in cellPair:
                        CGIs = list(pair)
                        if CGIs[0]  in self.CGI2GEO and  self.GEOSets[self.CGI2GEO[CGIs[0]]]['cell'][CGIs[0]].get('endPoint'):
                            if CGIs[1] in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGIs[1]]]['cell'][CGIs[1]].get('endPoint'):
                                endPoint1 = self.GEOSets[self.CGI2GEO[CGIs[0]]]['cell'][CGIs[0]].get('endPoint')
                                endPoint2 = self.GEOSets[self.CGI2GEO[CGIs[1]]]['cell'][CGIs[1]].get('endPoint')
                                miffp.write('Line {} {} {} {}\r\n'.format(endPoint1[0],endPoint1[1],endPoint2[0],endPoint2[1]))
                                miffp.write('    Pen (20,31,255)\r\n')
                                ARFCN =[int(arfcn) for arfcn in cellPair[pair] ]
                                CGIs.append(str(ARFCN).replace(',',':').replace('[','').replace(']','').replace(' ',''))
                                CGIs.append(0)
                                if ARFCN[0]  in range(1,125) or ARFCN[0] in range(1000,1025):
                                    CGIs.append('900M')
                                else:
                                    CGIs.append('1800M')
                                mid.writerow(CGIs )

        self.logger.info('Done.')

    def GenRFPlanChangeReport(self,filePath,org,new):
        self.logger.info('{} started..'.format('GenRFPlanChangeReport'))
        if filePath:
            with open(filePath,'wb') as fp:
                writer = csv.writer(fp)
                header = ['CGI','changed','orgPlan','alteredPlan']
                writer.writerow(header)
                for CGI in org:
                    row  = [CGI,org[CGI] == new.get(CGI),org[CGI],new.get(CGI)]
                    #TODO Add CoSite CoArfch and AdjArfcn check
                    writer.writerow(row)
        self.logger.info('{} done..'.format('GenRFPlanChangeReport'))

    def Console(self):
        FORMAT = '%(asctime)-15s %(levelname)s :%(message)s'
        formatter = logging.Formatter(fmt=FORMAT)
        logging.basicConfig(format=FORMAT,level=logging.INFO)
        log = logging.getLogger('global')
        PRSMR = False
        mmlfp = raw_input('Please input MML store path:')
        mrfp = raw_input('Please input M2000/PRS MR full name path[if use PRS MR just ENTER here]:')
        thres = raw_input('Please input MR correlate threshold for 12dB[default 0.03]:')
        cfgfp = raw_input('Please input Geo cfg path:')
        tafp = raw_input('Please input ERL and TA dist file:')
        recordHours = int(raw_input('How many sample hours in ERL file?'))
        self.GRID_SIZE = int(raw_input('Grid size in meters:'))
        self.MAX_BAND_WIDTH = int(raw_input('1800M Bandwith ,20 or 25?'))
        kpifp = raw_input('Please input performance file path:')
        if thres.strip():
            try:
                self.CORR_THRES = float(thres.strip())
                self.OVERLAP_COR_THRES = self.CORR_THRES
            except :
                self.logger.error('Invalid threshold {}.use default {}'.format(thres,self.CORR_THRES))
        FH = logging.FileHandler('{}.log'.format(mrfp),'wb')
        FH.setFormatter(formatter)
        #self.logger.addHandler(logging.StreamHandler())
        self.logger.addHandler(FH)
        self.logger.setLevel(logging.DEBUG)
        self.logger.info('Start')
        self.GeoCfgParser(cfgfp)
        self.MMLPaser(mmlfp)
        self.MRParser(mrfp)
        self.CellPerformanceParser(kpifp)
        self.MR_Nei_Match()
        self.reIndexGcellRelation(True)
        self.CalcARFCNConflictByNetwork()
        self.write_mapped_MR(mrfp)
        self.correlateDetect3()
        self.ClusterShrinkCHK()

        self.CalcCellCoverageBoundary(HalfCoverage = False)
        self.selfOutputCellCoverage(cfgfp)
        self.GenSiteMap(cfgfp)
        self.GridCoverageDetect()
        self.TA_ERL_Parser(tafp,recordHours)
        self.CalcGridErlDensity()
        self.CalcDelauNeis()
        self.CalcCellDist()
        self.OutputGridToMIF_new(tafp)
        self.RestoreCGIERL(tafp)
        self.output_cluterReport(mrfp)
        self.DrawConflictPairs(cfgfp)

        orgRFPlan = self.GenRFPlan()
        targetCells = [CGI for CGI in self.GCELL if CGI in self.CGI2GEO and self.GEOSets[self.CGI2GEO[CGI]]['cell'][CGI].get('type') != 'indoor']
        self.ConflictTCHResolver(self.arfcnlist_900+self.arfcnlist_1800,targetCells)
        alteredRFPlan = self.GenRFPlan()
        self.GenRFPlanChangeReport(mrfp,orgRFPlan,alteredRFPlan)

        self.CalcARFCNConflictByNetwork()
        self.output_cluterReport('{}.altered'.format(mrfp))
        self.DrawConflictPairs('{}.altered'.format(cfgfp))

        self.logger.info('All Done')

class MainController(object):

    def __init__(self):
        self.logger = logging.getLogger('global')
        self.mrAnalyzer = NSAnalyzerClass(self)
        self.fmtAnalyzer = TemsScanParserClass(self)
        self.MMLPath = None
        self.MRFiles = []
        self.TEMSLogs = []
        self.GSMCellFile = None
        self.GSMKPIFile = None
        self.GSMTrafficFile = None
        self.runningWorker = []
        self.locker = threading.RLock()
        self.dbLocker = threading.RLock()
        self.dbReferenced = 0
        self.progressQueue = _globals.getProgressQueue()

        self.__projectDBConn = None
        self.isProjectDbInMemory = False
        self.projectPath = None
        self.utmZone = 50
        self.mainWindow = None

        self.__dbVer = '1'

        self.cellCoverageResultDictIndex = ['validSamples','cvs',
                          'interferSamples','cis',
                          'dominateSamples','cds',
                          'overlappedInterferSamples','cois',
                          'overlappedDominateSamples','cods',
                          'sampleInBadSSI','csibssi',
                          'overlappedSampleInBadSSI','cosibssi'
        ]
        self.createCellCorrelationTableSql = 'CREATE TABLE IF NOT EXISTS cellCorrelation (scgi TEXT,ncgi TEXT,coe REAL)'
        self.createCellToClusterTableSql = 'CREATE TABLE IF NOT EXISTS cellToCluster (clusterid INTEGER,cgi TEXT)'
        self.createCellIntfInfoTableSql = 'CREATE TABLE IF NOT EXISTS cellIntfInfo (cgi TEXT PRIMARY KEY,CoFreqAffectingOthers REAL,CoFreqAffectedBy REAL,SrvCellInferiorTo12dBMRs REAL,SrvCellIntfToNeiMRsBy12dB REAL,SrvCellOverlapedNeis TEXT,SrvCellOverlapedNeisBeyondDelauny TEXT,SrvCellOverlapedTRXs INTEGER,SrvCellOverlapedTRXsBeyondDelNei INTEGER,SrvMRs REAL,TotalClusterCount INTEGER,CoArfcnCells INTEGER,CriticalClusterCount INTEGER)'

    def openProject(self,projectPath):
        '''OPENPROJECT'''
        #Above line should not be modified....
        if projectPath:
            dbInfo = self.__verifyDBEnvrionment(projectPath)
            if dbInfo:
                self.projectPath = projectPath
                self.utmZone = dbInfo.get('utmZone',50)
                self.projectName = dbInfo.get('projectName','Noname')

                self.fmtAnalyzer.maxBsicMatchRange = dbInfo.get('maxBsicMatchRange',5000)
                self.fmtAnalyzer.maxBcchMatchRange = dbInfo.get('maxBcchMatchRange',1000)
                if dbInfo.get('dataload'):
                    if type(dbInfo.get('dataload')) is list:
                        for dataload in dbInfo.get('dataload'):
                            if 'scanMatch' == dataload:
                                self.fmtAnalyzer.isMatched = True
                            elif 'cellIntfInfo' == dataload:
                                self.isCellIntfInfoLoaded = True
                            elif 'cellCorrelation' == dataload:
                                self.isCellCorrelationLoaded = True
                            elif 'geoInfo' == dataload:
                                self.isGeoInfoLoaded = True
                            elif 'MML' == dataload:
                                self.isMMLLoaded = True
                            elif 'cellToCluster' == dataload:
                                self.isCellToClusterLoaded = True
                            elif 'blackMR' == dataload:
                                self.isblackMRLoaded = True
                    else:
                        if 'scanMatch' == dbInfo.get('dataload'):
                            self.fmtAnalyzer.isMatched = True
                            self.logger.info('TEMS scan match sample detected.')
                self.__restoreNaDataStructureFromDB()
                self.logger.info('Project verify complete and successfully.')
                return True
            else:
                self.logger.error('Invalid project file.')
                return False
        else:
            self.logger.error('Project path can not be empty')
            return False

    def __verifyDBEnvrionment(self,targetPath):
        try:
            try:
                dbConn = None
                self.logger.debug('Try original db3 format.')
                dbConn = sqlite3.connect(targetPath)
                dbConn.execute('SELECT * FROM projectinfo')
            except sqlite3.DatabaseError:
                dbConn = None
                self.logger.debug('Try db3 dump format.')
                dbConn = self.__readDbFromDump(targetPath)
                self.isProjectDbInMemory = True
                self.__projectDBConn = dbConn
            if not dbConn:
                raise IOError,'Unable to connect to database'
            dbConn.row_factory = sqlite3.Row
            cursor = dbConn.cursor()
            dbInfo = {}
            for info in cursor.execute('SELECT * FROM projectinfo'):
                if info['attribute'] == 'dbVer' and info['value'] != self.__dbVer:
                    self.logger.warn('Unsupported database version.')
                    return False
                if info['attribute'] == u'dataload':
                    if u'dataload' in dbInfo:
                        dbInfo[u'dataload'].append(info['value'])
                    else:
                        dbInfo[u'dataload']=[info['value'],]
                else:
                    dbInfo[info['attribute']] = info['value']
            self.logger.debug('DB info:{}'.format(dbInfo))
            return dbInfo
        except:
            self.logger.exception('Unknow error occured when reading project info.')
            return False

    def createNewProject(self,projectPath,projectName = 'Noname',UTMZone = 50):
        if projectPath:
            if self.__newDBEnvrionment(dbName=projectPath,projName=projectName,UTMZone=UTMZone):
                self.projectPath = projectPath
        self.utmZone = UTMZone
        self.projectName = projectName

    def __readDbFromDump(self,targetPath):
        if targetPath:
            self.logger.debug(u'Reading dump file {}'.format(targetPath))
            try:
                #dbConn = sqlite3.connect('{}.db3'.format(targetPath))
                dbConn = sqlite3.connect(':memory:')
                cursor = dbConn.cursor()
                with bz2.BZ2File(targetPath,'r') as bz2file:
                    for sql in bz2file:
                        try:
                            cursor.execute(sql)
                        except sqlite3.OperationalError:
                            if sql.strip() == 'COMMIT;':
                                pass
                            else:
                                self.logger.exception(u'Failed SQL {}'.format(sql))
                self.logger.debug('Reading done.')
                dbConn.commit()
                return dbConn
            except:
                self.logger.exception('Unknown error occured when reading from dump data.')
                return False
        else:
            self.logger.error('Missing filepath,read from dump failed!')

    def _dumpDbToBz2(self,targetPath):
        dbConn = self._acquireDBConn(blocking = False)
        if dbConn:
            try:
                if targetPath:
                    if os.access(targetPath,os.F_OK):
                        os.remove(targetPath)
                    with bz2.BZ2File(targetPath,'w') as bz2file:
                        cache = []
                        for line in dumpSqlite3._iterdump(dbConn):
                            cache.append(line.encode('utf-8')+'\r\n')
                            if len(cache) > 100000:
                                bz2file.writelines(cache)
                                cache = []
                        bz2file.writelines(cache)
                    return True
            except WindowsError:
                self.logger.exception(u'Unable to write {},please check wether it is open.'.format(targetPath))
            except:
                self.logger.exception('Unexpect error occured during dumping project.')
            finally:
                self._releaseDBConn()
        elif dbConn == False:
            self.logger.error('Project database is locked by other operation.')
        elif dbConn == None:
            self.logger.error('Project database connection does not exist.')

        return False

    def saveAsProject(self,targetPath):
        if targetPath:
            return self._dumpDbToBz2(targetPath)

    def _acquireDBConn(self,blocking=True):
        if self.isProjectDbInMemory:
            if self.__projectDBConn:
                if self.dbLocker.acquire(blocking):
                    self.dbReferenced += 1
                    return self.__projectDBConn
                else:
                    return False
            else:
                return None
        elif self.projectPath :
            if self.dbLocker.acquire(blocking):
                #Using file dbconnection
                self.dbReferenced += 1
                self.__projectDBConn = sqlite3.connect(self.projectPath)
                return self.__projectDBConn
            else:
                return False
        else:
            return None

    def _releaseDBConn(self):
        try:
            if self.isProjectDbInMemory:
                if not self.dbReferenced:
                    pass
            else:
                if not self.dbReferenced:
                    conn = self.__projectDBConn
                    self.__projectDBConn = None
                    self.__projectDBConn.close(conn)
        except AttributeError:
            pass
        finally:
            try:
                self.dbLocker.release()
                if self.dbReferenced:
                    self.dbReferenced -= 1
            except:
                self.logger('Failed to realease db locker')

    def _closeDBConn(self):
        dbConn = self._acquireDBConn(False)
        if dbConn == False :
            self.logger.error('Project database is beening used by other thread,close failed.')
        elif self.isProjectDbInMemory and dbConn:
            dbConn.close()
            self.__projectDBConn = None
        self._releaseDBConn()

    def __newDBEnvrionment(self,dbName = 'default.db3',inMemory = False,projName = None,cellMaxRadius = 5000,UTMZone = 50,Ellips = 'WGS84'):
        """"""
        self.logger.info('Creating %s' % dbName)
        #self.projectPath = dbName
        try:
            if inMemory:
                dbConn = sqlite3.connect(':memory:')
            else:
                if os.access(dbName,os.F_OK):
                    os.remove(dbName)
                dbConn = sqlite3.connect(dbName)
                #dbConn.execute('DROP TABLE IF EXISTS projectinfo')
            dbConn.execute('CREATE TABLE projectinfo (attribute TEXT,value TEXT,timestamp TEXT)')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dbVer","1",datetime("now"))')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("projectName",?,datetime("now"))',(projName,))
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("utmZone","50",datetime("now"))')
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("ellips",?,datetime("now"))',(Ellips,))
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("maxBsicMatchRange",?,datetime("now"))',(self.fmtAnalyzer.maxBsicMatchRange,))
            dbConn.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("maxBcchMatchRange",?,datetime("now"))',(self.fmtAnalyzer.maxBcchMatchRange,))
            dbConn.commit()
            self.logger.info('Project file created successfully...')
            if inMemory:
                self.isProjectDbInMemory = True
                self.__projectDBConn = dbConn
            else:
                dbConn.close()
            return True
        except WindowsError:
            self.logger.exception('Failed to create project file,please check if it is open')
            return False

    def getCgiList(self):
        #return self.mrAnalyzer.GCELL
        dbConn = self._acquireDBConn(blocking = False)
        try:
            if dbConn:
                sql = 'SELECT cgi FROM cellNetworkInfo'
                cursor = dbConn.cursor()
                cursor.execute(sql)
                cgi = [row[0] for row in cursor]
                return cgi
            else:
                return False
        except:
            self.logger.exception('Unexpected error occured when execute SQL:{} !'.format(sql))
        finally:
            self._releaseDBConn()

    def getGSMCellNetworkInfo(self):
        #return self.mrAnalyzer.GCELL
        dbConn = self._acquireDBConn(blocking = False)
        try:
            if dbConn:
                sql = 'SELECT cgi,bcch,x,y,dir,coverage_type,longitude,latitude,cellname,bsic,extcell,tile,projx,projy FROM cellNetworkInfo'
                cursor = dbConn.cursor()
                cursor.execute(sql)
                return cursor.fetchall()
            else:
                self.logger.error('Database connection can not be acquired.')
                return False
        except:
            self.logger.exception('Unexpected error occured when execute SQL:{} !'.format(sql))
        finally:
            self._releaseDBConn()

    def getGsmCellDetailedInfo(self,cgi):
        dbConn = self._acquireDBConn(blocking = False)
        try:
            if dbConn:
                sql = 'SELECT cell.cgi,cell.bcch,cell.dir,cell.coverage_type,cell.cellname,cell.bsic,cell.extcell,cell.tile,int.srvmrs,int.srvcelloverlapedNeis , int.srvcelloverlapedneisbeyonddelauny,int.Coarfcncells, int.totalclustercount AS totalCluster, int.criticalclustercount AS criticalCluster,cell.height FROM cellNetworkInfo AS cell LEFT JOIN cellIntfInfo AS int ON int.cgi = cell.cgi WHERE int.cgi = ?'
                cursor = dbConn.cursor()
                cursor.execute(sql,[cgi,])
                result = cursor.fetchone()
                if result:
                    cellInfo = {}
                    cellInfo['cgi'] = result[0]
                    cellInfo['bcch'] = result[1]
                    cellInfo['dir'] = result[2]
                    cellInfo['type'] = result[3]
                    cellInfo['name'] = result[4]
                    cellInfo['bsic'] = result[5]
                    cellInfo['tile'] = result[7]
                    cellInfo['mrs'] = result[8]
                    cellInfo['covered'] = result[9].count(',')+1
                    cellInfo['overlapped'] = result[10].count(',')+1
                    cellInfo['coArfcnCells'] = result[11]
                    cellInfo['cluster'] = result[12]
                    cellInfo['pcluster'] = result[13]
                    cellInfo['height'] = result[14]
                    tchSql = 'SELECT arfcn,type FROM cellTrx WHERE cgi = ?'
                    cursor.execute(tchSql,[cgi,])
                    cellInfo['tchs'] = [row[0] for row in cursor if row[0] != "NonBCCH"]
                    return cellInfo
                else:
                    return None
            else:
                self.logger.error('Database connection can not be acquired.')
                return False
        except:
            self.logger.exception('Unexpected error occured when execute SQL:{} !'.format(sql))
        finally:
            self._releaseDBConn()

    def getFMTScanSampleByCoords(self,longitude,latitude):
        if not self.fmtAnalyzer.isMatched:
            self.logger.error('FMT Scan sample is not loaded!')
            return False
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            sampleSelectSql = 'SELECT fmt.cgi,fmt.bcch,fmt.bsic,fmt.arxlev,fmt.samples,fmt.distance,fmt.ref_distance,cell.tch_count FROM raw_fmt_aggregate as fmt LEFT JOIN cellNetworkInfo as cell ON fmt.cgi = cell.cgi WHERE fmt.longitude = ? AND fmt.latitude = ? ORDER BY fmt.arxlev DESC'
            sampleCursor = dbConn.cursor()
            samples = dict(samples = [],maxRxlev = 0)
            for clip in sampleCursor.execute(sampleSelectSql,(longitude,latitude)):
                samples['samples'].append([value for value in clip])
                samples['maxRxlev'] = max(samples['maxRxlev'],clip[3])
            return samples
        except:
            self.logger.exception('Unexpected error occured when select TEMS scan sample by Coords.')
            return False
        finally:
            self._releaseDBConn()

    def getFMTScanSampleByMatchedCGI(self,cgi):
        """
        :rtype : list[(longitude,latitude,x,y,arxlev,bsic,distance,ref_distance,maxRxlev)]
        """
        if not self.fmtAnalyzer.isMatched:
            self.logger.error('FMT Scan sample is not loaded!')
            return False
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            sampleSelectSql = 'SELECT longitude,latitude,x,y,arxlev,bsic,distance,ref_distance,projx,projy FROM raw_fmt_aggregate WHERE cgi = "{}"'
            sampleCursor = dbConn.cursor()
            samples = []
            #self.logger.debug('SQL:{}'.format(sampleSelectSql.format(cgi)))
            for clip in sampleCursor.execute(sampleSelectSql.format(cgi)):
                maxRxlevSql = 'SELECT arxlev FROM raw_fmt_aggregate WHERE longitude = ? AND latitude = ? ORDER BY arxlev DESC LIMIT 1'
                innerCursor = dbConn.cursor()
                innerCursor.execute(maxRxlevSql,(clip[0],clip[1]))
                result = innerCursor.fetchone()
                if result:
                    samples.append([value for value in clip]+[result[0],])
                else:
                    self.logger.error('Failed to get CGI {} coverage sample due to missing maxRxlev'.format(cgi))
                    return False
            return samples
        except:
            self.logger.exception('Unexpected error occured when select TEMS scan sample by CGI.')
            return False
        finally:
            self._releaseDBConn()

    def getFMTScanedSamples(self):
        self.logger.debug('get sample started.')
        if not self.fmtAnalyzer.isMatched:
            self.logger.error('FMT Scan sample is not loaded!')
            return False
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            uniqueGeoSql = 'SELECT DISTINCT longitude,latitude,x,y,projx,projy FROM raw_fmt_aggregate'
            sampleSelectSql = 'SELECT cgi,bcch,bsic,arxlev,samples,distance,ref_distance FROM raw_fmt_aggregate WHERE longitude = ? AND latitude = ?'
            geoCursor = dbConn.cursor()
            sampleCursor = dbConn.cursor()
            geoCursor.execute(uniqueGeoSql)
            dataSet = []
            for geoRow in geoCursor:
                sample = {}
                sample['x'] = geoRow[2]
                sample['y'] = geoRow[3]
                sample['projx'] = geoRow[4]
                sample['projy'] = geoRow[5]
                sample['longitude'] = geoRow[0]
                sample['latitude'] = geoRow[1]
                sample['samples'] = []
                sample['maxRxlev'] = 0
                #self.logger.debug('loaded {}'.format(sample))
                sampleCursor.execute(sampleSelectSql,(geoRow[0],geoRow[1]))
                for clip in sampleCursor:
                    sample['samples'].append([value for value in clip])
                    sample['maxRxlev'] = max(sample['maxRxlev'],clip[3])
                dataSet.append(sample)
            return dataSet
        finally:
            self._releaseDBConn()

    def getFMTScanedSamplesIterator(self):
        if not self.fmtAnalyzer.isMatched:
            return
        dbConn = self._acquireDBConn()
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return
        try:
            #uniqueGeoSql = 'SELECT DISTINCT longitude,latitude,x,y FROM raw_fmt_aggregate'
            uniqueGeoSql = 'SELECT DISTINCT longitude,latitude,x,y,projx,projy FROM raw_fmt_aggregate'
            sampleSelectSql = 'SELECT cgi,bcch,bsic,arxlev,samples,distance,ref_distance FROM raw_fmt_aggregate WHERE longitude = ? AND latitude = ?'
            geoCursor = dbConn.cursor()
            sampleCursor = dbConn.cursor()
            geoCursor.execute(uniqueGeoSql)
            for geoRow in geoCursor:
                sample = {}
                sample['x'] = geoRow[2]
                sample['y'] = geoRow[3]
                sample['projx'] = geoRow[4]
                sample['projy'] = geoRow[5]
                sample['longitude'] = geoRow[0]
                sample['latitude'] = geoRow[1]
                sample['samples'] = []
                sample['maxRxlev'] = 0
                sampleCursor.execute(sampleSelectSql,(geoRow[0],geoRow[1]))
                for clip in sampleCursor:
                    sample['samples'].append([value for value in clip])
                    sample['maxRxlev'] = max(sample['maxRxlev'],clip[3])
                yield sample
        finally:
            self._releaseDBConn()

    def getDelnySurrondingCells(self,watchSpotXY,surrondingCells):
        #self.logger.info('Calculate street structual index started..')
        """
        calc delny surronding cell by utm project system
        """
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            sql = 'SELECT cgi,x,y,longitude,latitude FROM cellNetworkInfo WHERE coverage_type = "macro" AND x NOTNULL AND cgi in {}'
            #sql = 'SELECT cgi,projx,projy,longitude,latitude FROM cellNetworkInfo WHERE coverage_type = "macro" AND projx NOTNULL AND cgi in {}'
            cellListStr = '("' + '","'.join(surrondingCells) + '")'
            #self.logger.debug('SQL: {}'.format(sql.format(cellListStr)))
            cursor.execute(sql.format(cellListStr))
            result = cursor.fetchall()
            if result:
                sets = set([(row[1],row[2]) for row in result])
                sets.add(watchSpotXY)
                relation = delaunay.Triangulation(list(sets)).get_neighbours()
                minimumNei = [row[0] for row in result if (row[1],row[2]) in relation[watchSpotXY]]
                return minimumNei
            else:
                return False
        except:
            self.logger.exception('Unexpected error occured found when getDelnySurrondingCells!')
            self.logger.exception('{} Sets {}'.format(watchSpotXY,sets))
            return False
        finally:
            self._acquireDBConn()
            #self.logger.info('Calculate street structual index done.')

    def getCoveredNeisReCalc(self,cgi,gtCOE = 0.03):
        self.logger.debug('getCoveredNeis at {}'.format(cgi))
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            dataSet = []
            cursor = dbConn.cursor()
            cursor.execute('SELECT scgi FROM cellCorrelation WHERE ncgi = ? and coe > ?',(cgi,gtCOE))
            for row in cursor:
                dataSet.append(row[0])
            return dataSet
        except:
            self.logger.exception('Unexpected error occured found when getCoveredNeisReCalc!')
        finally:
            self._releaseDBConn()

    def getCoveredNeisByHist(self,cgi,gtCOE = 0.03):
        #self.logger.debug('getCoveredNeis at {}'.format(cgi))
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            cursor.execute('SELECT SrvCellOverlapedNeis FROM cellIntfInfo WHERE cgi = ?',(cgi,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0].split(',')
            else:
                return self.getCoveredNeisReCalc(cgi,gtCOE)
        except:
            self.logger.exception('Unexpected error occured found when getCoveredNeisByHist!')
        finally:
            self._releaseDBConn()

    def getOverlayedNeisByHist(self,cgi):
        #self.logger.debug('getCoveredNeis at {}'.format(cgi))
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            cursor.execute('SELECT SrvCellOverlapedNeisBeyondDelauny FROM cellIntfInfo WHERE cgi = ?',(cgi,))
            result = cursor.fetchone()
            if result and result[0]:
                return result[0].split(',')
            else:
                return []
        except:
            self.logger.exception('Unexpected error occured found when getOverlayedNeisByHist!')
        finally:
            self._releaseDBConn()

    def getCellConflictArfcnPairs(self,cgi,gtCOE=0.03):
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            sql = 'SELECT cgi,arfcn,type FROM "cellTrx"  WHERE cgi IN (SELECT ncgi as intcgi FROM "cellCorrelation" WHERE coe > ? AND scgi = ? UNION SELECT scgi AS intcgi FROM "cellCorrelation" WHERE coe > ? AND ncgi = ? ) AND arfcn IN (SELECT arfcn FROM "cellTrx" WHERE cgi = ? )'
            #sql = 'SELECT cgi,arfcn,type FROM "cellTrx"  WHERE cgi IN (SELECT ncgi as intcgi FROM "cellCorrelation" WHERE coe > {} AND scgi = "{}" UNION SELECT scgi AS intcgi FROM "cellCorrelation" WHERE coe > {} AND ncgi = "{}" ) AND arfcn IN (SELECT arfcn FROM "cellTrx" WHERE cgi = "{}" )'.format(gtCOE,cgi,gtCOE,cgi,cgi)
            #cursor.execute('SELECT cgi,arfcn,type FROM "cellTrx"  WHERE cgi IN (SELECT ncgi as intcgi FROM "cellCorrelation" WHERE coe > ? AND scgi = ? UNION SELECT scgi AS intcgi FROM "cellCorrelation" WHERE coe > ? AND ncgi = ? ) AND arfcn IN (SELECT arfcn FROM "cellTrx" WHERE cgi = ? )',(gtCOE,cgi,gtCOE,cgi,cgi))
            cursor.execute(sql,(gtCOE,cgi,gtCOE,cgi,cgi))
            #self.logger.debug('Fire {}'.format(sql))
            result = cursor.fetchall()
            #self.logger.debug('Got {}'.format(result))
            return result
        except:
            self.logger.exception('Unexpected error occured found when getCellConflictArfcnPairs!')
        finally:
            self._releaseDBConn()

    def getCellsByClusterID(self,clusterid):
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            cursor.execute('SELECT cgi FROM cellToCluster WHERE clusterid = ?',(clusterid,))
            return cursor.fetchall()
        except:
            self.logger.exception('Unexpected error occured found when getCellsByClusterID!')
        finally:
            self._releaseDBConn()

    def getRelatedClustersByCgi(self,cgi):
        self.logger.info('Get Related Clusters by CGI started..')
        dbConn = self._acquireDBConn()
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            dataSet = {}
            cursor = dbConn.cursor()
            clusterScaleSql = 'SELECT scale.clusterid,scale.cells,trx.trxs FROM (SELECT cluster.clusterid,count(cluster.cgi) as cells FROM cellToCluster as cluster WHERE cluster.clusterid in (SELECT clusterid FROM cellToCluster WHERE cgi = ? ) GROUP BY cluster.clusterid ) AS scale JOIN (SELECT cluster.clusterid,count(cellTrx.cgi) as trxs FROM cellToCluster as cluster LEFT JOIN cellTrx ON cluster.cgi = cellTrx.cgi WHERE cluster.clusterid in (SELECT clusterid FROM cellToCluster WHERE cgi = ? ) GROUP BY cluster.clusterid) AS trx ON trx.clusterid = scale.clusterid'
            clusterComplexedSql = 'SELECT scale.clusterid,scale.cells as cells,scale.trxs as trxs,conflict.arfcns as conflict_arfcns,conflict.trxs as conflict_trxs FROM (SELECT scale.clusterid as clusterid,scale.cells as cells ,trx.trxs as trxs FROM (SELECT cluster.clusterid,count(cluster.cgi) as cells FROM cellToCluster as cluster WHERE cluster.clusterid in (SELECT clusterid FROM cellToCluster WHERE cgi = ?) GROUP BY cluster.clusterid ) AS scale JOIN (SELECT cluster.clusterid,count(cellTrx.cgi) as trxs FROM cellToCluster as cluster LEFT JOIN cellTrx ON cluster.cgi = cellTrx.cgi WHERE cluster.clusterid in (SELECT clusterid FROM cellToCluster WHERE cgi = ?) GROUP BY cluster.clusterid) AS trx ON trx.clusterid = scale.clusterid) as scale LEFT JOIN (SELECT clusterid,sum(arfcnCount) as trxs,count(arfcn)as arfcns FROM (SELECT cluster.clusterid,count(cellTrx.arfcn) as arfcnCount,cellTrx.arfcn  FROM cellToCluster as cluster LEFT JOIN cellTrx ON cluster.cgi = cellTrx.cgi WHERE cluster.clusterid in (SELECT clusterid FROM cellToCluster WHERE cgi = ?) GROUP BY cluster.clusterid,cellTrx.arfcn) AS arfcnConflict WHERE arfcnCount > 1 GROUP BY clusterid) AS conflict on conflict.clusterid = scale.clusterid'
            cursor.execute(clusterComplexedSql,(cgi,cgi,cgi))
            return cursor.fetchall()
        except:
            self.logger.exception('Unexpected error occured found when calculate getRelatedClustersByCgi!')
            return False
        finally:
            self._releaseDBConn()
            self.logger.info('Get Related Clusters By Cgi done.')

    def getGlobalProblemClusters(self,THRES_900 = 60,THRES_1800 = 80):
        self.logger.info('Get problem clusters by CGI started,trxs threshold for 900:{},1800:{}'.format(THRES_900,THRES_1800))
        dbConn = self._acquireDBConn()
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            dataSet = {}
            cursor = dbConn.cursor()
            clusterComplexedSql = 'SELECT scale.clusterid as clusterid,scale.cells as cells ,trx.trxs as trxs,avgbcch.avg_arfcn as avg_bcch FROM (SELECT cluster.clusterid,count(cluster.cgi) as cells FROM cellToCluster as cluster  GROUP BY cluster.clusterid ) AS scale JOIN (SELECT cluster.clusterid,count(cellTrx.cgi) as trxs FROM cellToCluster as cluster LEFT JOIN cellTrx ON cluster.cgi = cellTrx.cgi GROUP BY cluster.clusterid) AS trx ON trx.clusterid = scale.clusterid LEFT JOIN (SELECT cluster.clusterid as clusterid,avg(trx.arfcn) as avg_arfcn FROM cellToCluster as cluster LEFT JOIN cellTrx AS trx ON cluster.cgi = trx.cgi WHERE trx.type = "BCCH" GROUP BY cluster.clusterid) AS avgbcch ON scale.clusterid = avgbcch.clusterid WHERE (avg_bcch < 125 AND trxs > ?) OR (avg_bcch >= 512 AND trxs > ?)'
            cursor.execute(clusterComplexedSql,(THRES_900,THRES_1800))
            return cursor.fetchall()
        except:
            self.logger.exception('Unexpected error occured found when get Problem Clusters!')
            return False
        finally:
            self._releaseDBConn()
            self.logger.info('Get Problem Clusters done.')

    def getGlobalCoArfcnPairs(self,gtCOE=0.03,TrxType = 'NonBCCH',BAND = u'900M'):
        """GLOBAL_COARFCN_CHECK"""
        self.logger.info('Check global CoArfcn started...')
        if self.mrAnalyzer.CalcARFCNConflictByNetwork():
            conflictPair = set()
            for CGI in self.mrAnalyzer.GCELL:
                try:
                    if BAND ==u'900M' and int(self.mrAnalyzer.GCELL[CGI].get('BCCH')) > 125:
                        continue
                    if BAND ==u'1800M' and int(self.mrAnalyzer.GCELL[CGI].get('BCCH')) < 512:
                        continue
                except:
                    self.logger.warn('Unknown band type for CGI {},BCCH {}'.format(CGI,self.mrAnalyzer.GCELL[CGI].get('BCCH')))
                CoPairs = self.mrAnalyzer.GCELL[CGI].get('CoArfcnCell')
                if CoPairs:
                    for item in CoPairs:
                        conflictPair.add(frozenset([CGI,item]))
            self.logger.info('Check global CoArfcn completed.')
            return conflictPair
        else:
            return False

    def __getGlobalCoArfcnPairs(self,gtCOE=0.03,TrxType = 'NonBCCH'):
        """GLOBAL_COARFCN_CHECK"""
        self.logger.info('Get global {} CoArfcn paris started,gtCOE:{}'.format(TrxType,gtCOE))
        dbConn = self._acquireDBConn()
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            detected = {}
            dataSet = []
            globalCursor = dbConn.cursor()
            detectCursor = dbConn.cursor()
            coeQualifiedPairs = 'SELECT scgi,ncgi FROM cellCorrelation WHERE coe > ?'
            conflictDetect = 'SELECT arfcn FROM "cellTrx" WHERE cgi = ? AND type = ? INTERSECT SELECT arfcn FROM "cellTrx" WHERE cgi = ? AND type = ?'
            globalCursor.execute(coeQualifiedPairs,(gtCOE,))
            cellPair = globalCursor.fetchall()
            count = 0
            maxCount = len(cellPair)
            for pair in cellPair:
                pairset = frozenset((pair[0],pair[1]))
                if pairset not in detected:
                    detected[pairset] = 1
                    #self.logger.debug((pair[0],TrxType,pair[1],TrxType))
                    detectCursor.execute(conflictDetect,(pair[0],TrxType,pair[1],TrxType))
                    result = detectCursor.fetchall()
                    if result:
                        dataSet.append((pair[0],pair[1]))
                count += 1
                self.progressQueue.put(count*100/maxCount)
            self.progressQueue.put(100)
            return dataSet
        except:
            self.logger.exception('Unexpected error occured found when getGlobalCoArfcnPairs!')
            return False
        finally:
            self._releaseDBConn()
            self.logger.info('get Global CoArfcn Pairs done.')

    def startRFRePlanProcedure(self,filePath,arfcns,targetCells):
        self.logger.info('Start RF RePlan Procedure,report saving at {}'.format(filePath))

        self.logger.info('Start conflict detect.')
        self.mrAnalyzer.CalcARFCNConflictByNetwork()
        self.logger.info('Complete conflict detect.')
        self.logger.info('Copy orginal RF Plan.')
        orgPlan = self.mrAnalyzer.GenRFPlan()
        self.logger.info('Copy orginal RF Plan complete.')
        self.logger.info('Start TCH conflict resolver,long run...')
        if self.mrAnalyzer.ConflictTCHResolver(arfcns,targetCells):
            self.logger.info('TCH conflict resolver complete.')
            self.logger.info('Generate new RF Plan...')
            newPlan = self.mrAnalyzer.GenRFPlan()
            self.logger.info('Generate new RF Plan complete.')
            self.logger.info('Generate RF RePlan report,at {}.'.format(filePath))
            self.mrAnalyzer.GenRFPlanChangeReport(filePath,orgPlan,newPlan)
            self.logger.info('Generate RF RePlan report complete.')
            self.logger.info('Complete RF RePlan Procedure.')
            return True
        else:
            self.logger.info('TCH conflict resolver failed.')
            return False


    def calcStreetStructualFeature(self,inferiorThreshold = 12):
        self.logger.info('Calculate street structual index started..')
        dbConn = self._acquireDBConn()
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            dataSet = {}
            cursor = dbConn.cursor()
            sql = 'SELECT cgi,tch_count FROM cellNetworkInfo WHERE cgi in (SELECT DISTINCT cgi FROM raw_fmt_aggregate WHERE longitude = ? AND latitude = ? and arxlev >= ?) GROUP BY cgi'
            #self.logger.debug('Try getFMTScanedSamplesIterator')
            for sample in  self.getFMTScanedSamplesIterator():
                #self.logger.debug('Looping...')
                cursor.execute(sql,(sample['longitude'],sample['latitude'],sample['maxRxlev'] - inferiorThreshold))
                result = cursor.fetchall()
                if result:
                    trxCount = sum([row[1] for row in result if row[1]])
                    cellCount = len(result)
                    #TODO FIXME
                    dataSet[(sample['longitude'],sample['latitude'])] = ((sample['x'],sample['y']),trxCount,cellCount,(sample['projx'],sample['projy']))
                    #self.logger.debug('Hit.')
            self.logger.info('Get success result.')
            return dataSet
        except:
            self.logger.exception('Unexpected error occured found when calculate street structual index!')
            return False
        finally:
            self._releaseDBConn()
            self.logger.info('Calculate street structual index done.')

#    def DataLoadHandler(self,dataToLoad):
#        self.logger.debug(u'Load {} dispatched'.format(dataToLoad))
#        thread = threading.Thread(name='DataParser',target = self.dataParserThread,args=(dataToLoad,))
#        self.runningWorker.append(thread)
#        thread.start()

    def __restoreNaDataStructureFromDB(self):
        self.logger.info('Restore NaData Structure From DB started..')
        dbConn = self._acquireDBConn()
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            if hasattr(self,'isMMLLoaded') and self.isMMLLoaded:
                self.mrAnalyzer.GCELL = {}
                self.mrAnalyzer.ARFCN_IDX = {}
                self.mrAnalyzer.BCCH_BSIC_IDX = {}
                sql = 'SELECT cgi,cellname,bcch,bsic,extcell FROM cellNetworkInfo'
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    count = 0
                    maxCount = len(result)
                    self.logger.info('Restoring GCELL...')
                    for row in result:
                        self.mrAnalyzer.GCELL[row[0]] = dict(CGI = row[0],NAME = row[1],BCCH = '{}'.format(row[2]),NCC = '{}'.format(row[3][0]),BCC = '{}'.format(row[3][1]))
                        if row[4] == 1:
                            self.mrAnalyzer.GCELL[row[0]]['EXTCELL'] = True
                            #self.mrAnalyzer.GCELL[row[0]]['TRX'] = {'{}'.format(row[2]):-1}
                        try:
                            self.mrAnalyzer.BCCH_BSIC_IDX['{}#{}#{}'.format(row[2],row[3][0],row[3][1])].add(row[0])
                        except :
                            self.mrAnalyzer.BCCH_BSIC_IDX['{}#{}#{}'.format(row[2],row[3][0],row[3][1])] = set([row[0],])
                        count += 1
                        self.progressQueue.put(count*100/maxCount)
                    self.logger.info('Restoring GCELL complete.')

                sql = 'SELECT cgi,arfcn FROM cellTrx'
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    count = 0
                    maxCount = len(result)
                    self.logger.info('Restoring GTRX...')
                    for row in result:
                        try:
                            self.mrAnalyzer.GCELL[row[0]]['TRX']['{}'.format(row[1])] = -100
                        except KeyError:
                            self.mrAnalyzer.GCELL[row[0]]['TRX']={'{}'.format(row[1]):-100}
                        try:
                            self.mrAnalyzer.ARFCN_IDX['{}'.format(row[1])].append(row[0])
                        except KeyError:
                            self.mrAnalyzer.ARFCN_IDX['{}'.format(row[1])]=[row[0],]
                        count += 1
                        self.progressQueue.put(count*100/maxCount)
                    self.logger.info('Restoring GCELL complete.')

            if hasattr(self,'isGeoInfoLoaded') and self.isGeoInfoLoaded:
                self.mrAnalyzer.GEOSets = {}
                self.mrAnalyzer.CGI2GEO = {}
                sql = 'SELECT cgi,longitude,latitude,x,y,projx,projy,dir,tile,coverage_type,height FROM cellNetworkInfo'
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    count = 0
                    maxCount = len(result)
                    self.logger.info('Restoring GeoSet...')
                    for row in result:
                        if not row[1] or not row[2]:
                            count += 1
                            continue
                        try:
                            self.mrAnalyzer.GEOSets[(row[1],row[2])]['cell'][row[0]] = {'type':row[9],'dir':row[7],'tile':row[8],'height':row[10]}
                        except KeyError:
                            self.mrAnalyzer.GEOSets[(row[1],row[2])] = {}
                            self.mrAnalyzer.GEOSets[(row[1],row[2])]['cell'] = {}
                            self.mrAnalyzer.GEOSets[(row[1],row[2])]['cell'][row[0]] = {'type':row[9],'dir':row[7],'tile':row[8],'height':row[10]}

                            self.mrAnalyzer.GEOSets[(row[1],row[2])]['CoverageType'] = set()
                            self.mrAnalyzer.GEOSets[(row[1],row[2])]['coords'] = (row[3],row[4])
                            self.mrAnalyzer.GEOSets[(row[1],row[2])]['mapCoords'] = (row[5],row[6])
                        finally:
                            self.mrAnalyzer.GEOSets[(row[1],row[2])]['CoverageType'].add(row[9])
                            self.mrAnalyzer.CGI2GEO[row[0]] = (row[1],row[2])
                        count += 1
                        self.progressQueue.put(count*100/maxCount)
                    self.logger.info('Restoring GeoSet complete.')

            if hasattr(self,'isCellCorrelationLoaded') and self.isCellCorrelationLoaded:
                self.mrAnalyzer.CorrelateMatrixBy12dB = {}
                sql = 'SELECT scgi,ncgi,coe FROM cellCorrelation'
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    count = 0
                    maxCount = len(result)
                    self.logger.info('Restoring CorrelateMatrixBy12dB...')
                    for row in result:
                        try:
                            self.mrAnalyzer.CorrelateMatrixBy12dB[row[0]].add(row[1])
                        except KeyError:
                            self.mrAnalyzer.CorrelateMatrixBy12dB[row[0]] = set()
                            self.mrAnalyzer.CorrelateMatrixBy12dB[row[0]].add(row[1])
                        try:
                            self.mrAnalyzer.CorrelateMatrixBy12dB[row[1]].add(row[0])
                        except KeyError:
                            self.mrAnalyzer.CorrelateMatrixBy12dB[row[1]] = set()
                            self.mrAnalyzer.CorrelateMatrixBy12dB[row[1]].add(row[0])
                        count += 1
                        self.progressQueue.put(count*100/maxCount)
                    self.logger.info('Restoring CorrelateMatrixBy12dB complete.')

            if hasattr(self,'isCellIntfInfoLoaded') and self.isCellIntfInfoLoaded:
                self.logger.info('Restoring ServMRs complete.')
                self.mrAnalyzer.ServCellMR = {}
                sql = 'SELECT cgi,srvMrs FROM "cellIntfInfo" WHERE srvMrs >= 0;'
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    count = 0
                    maxCount = len(result)
                    self.logger.info('Restoring SrvMRs...')
                    for row in result:
                        self.mrAnalyzer.ServCellMR[row[0]] = row[1] * 6
                        count += 1
                        self.progressQueue.put(count*100/maxCount)
                    self.logger.info('Restoring ServMRs complete.')

        except:
            self.logger.exception('Unexpected error occured found when __restoreNaDataStructureFromDB!')
            return False
        finally:
            self.logger.info('Restore NaData Structure From DB done.')
            self._releaseDBConn()


    def dataParserThread(self,dataToLoad):
        '''DATAPARSER'''
        __doc__ = 'ggg'
        self.logger.info('Data parser thread entered.')

        target = dataToLoad.get('MmlPath',None)
        if target and target != self.MMLPath:
            self.mrAnalyzer._ResetMMLDataSet()
            self.MMLPath = target
            self.mrAnalyzer.MMLPaser(self.MMLPath)
        elif target:
            self.logger.warn(u'Directory {} already loaded,reload avoid.'.format(target))

        target = dataToLoad.get('MrFiles',None)
        if target and target != self.MRFiles:
            self.mrAnalyzer._ResetMRDataSet()
            self.MRFiles = target
            self.mrAnalyzer.MRParser(self.MRFiles)
        elif target:
            self.logger.warn(u'File {} already loaded,reload avoid.'.format(target))

        target = dataToLoad.get('GSMCellFile',None)
        if  target and target != self.GSMCellFile:
            self.mrAnalyzer._ResetGeoCfgDataSet()
            self.GSMCellFile = target
            self.mrAnalyzer.GeoCfgParser(self.GSMCellFile,zone = 50)
        elif target:
            self.logger.warn(u'File {} already loaded,reload avoid.'.format(target))

        target = dataToLoad.get('GSMKPIFile',None)
        if target and target  != self.GSMKPIFile:
            self.mrAnalyzer._ResetCellPerformanceDataSet()
            self.GSMKPIFile = target
            self.mrAnalyzer.CellPerformanceParser(self.GSMKPIFile)
        elif target:
            self.logger.warn(u'File {} already loaded,reload avoid.'.format(target))

        target = dataToLoad.get('GSMTrafficFile',None)
        if target and target  != self.GSMTrafficFile:
            self.mrAnalyzer._ResetCellTrafficDataSet()
            self.GSMTrafficFile = target
            #TODO change 6 to an user defined input
            self.mrAnalyzer.TA_ERL_Parser(self.GSMTrafficFile,6)
        elif target:
            self.logger.warn(u'File {} already loaded,reload avoid.'.format(target))

        target = dataToLoad.get('TemsLogs',None)
        if target and target  != self.TEMSLogs:
            self.TEMSLogs = target
            if self.fmtAnalyzer.fmtParser(target):
                if self.fmtAnalyzer._fmt_average():
                    #self.fmtAnalyzer.fmtSampleCGIMatch()
                    self.fmtAnalyzer.fmtSampleCGIMatchCached()
                else:
                    self.logger.error('Failed to average all FMT samples,CGI matching aborted!')
            else:
                self.logger.error('Failed to parser all FMT files,post process aborted!')
        elif target:
            self.logger.warn(u'File {} already loaded,reload avoid.'.format(target))

        self.logger.info('Data parser thread exit.')
        return True

    def calcDelauNeis(self):
        self.mrAnalyzer.CalcDelauNeis()

    def getSiteDistbyCGI(self,CGI,base = 500):
        return self.mrAnalyzer.getCellDistGradeByDelny(CGI,base)

    def calcMaximalConnectedCluster(self):
        self.logger.info('Start calculating maximal connected cluster...')
        if not self.mrAnalyzer.isMrMatched:
            if not self.mrAnalyzer.RAW_MR:
                self.logger.error('No RAW_MR exist,please parser MR first!')
                return
            self.mrAnalyzer.MR_Nei_Match()
            self.mrAnalyzer.blackMrFiltering()
            self.mrAnalyzer.resizeRawMrSpace()
        if not self.mrAnalyzer.isMrPostProcessed:
            self.mrAnalyzer.reIndexGcellRelation()
        if not self.mrAnalyzer.isMaximalConnectClusterDetected:
            self.mrAnalyzer.CalcMaximalConnectCluster()
        self.mrAnalyzer.CalcDelauNeis()
        self.logger.info('Calculating maximal connected cluster complete.')
        self.storeClusterCalculationToDb()
        self.storeCellCoverageInfoToDb()

    def calcSaveMaximalConnectedClusterReport(self,mrMatchOutputPath,clusterReportPath):
        self.logger.info('Start calculating maximal connected cluster,saving at {},{}'.format(mrMatchOutputPath,clusterReportPath))
        if not self.mrAnalyzer.isMrMatched:
            if not self.mrAnalyzer.RAW_MR:
                self.logger.error('No RAW_MR exist,please parser MR first!')
                return
            self.mrAnalyzer.MR_Nei_Match()
            self.mrAnalyzer.blackMrFiltering()
        if not self.mrAnalyzer.isMrPostProcessed:
            self.mrAnalyzer.reIndexGcellRelation()
        self.mrAnalyzer.improved_write_mapped_MR(mrMatchOutputPath)
        #self.mrAnalyzer.CalcARFCNConflictByNetwork() #included in mrAnalyzer.calcMrQual
        #self.mrAnalyzer.write_mapped_MR(mrMatchOutputPath) #SLOW
        #self.mrAnalyzer.correlateDetect3() #Fast but not accurate
        #self.mrAnalyzer.output_cluterReport(clusterReportPath) #for correlateDetect3()
        if not self.mrAnalyzer.isMaximalConnectClusterDetected:
            self.mrAnalyzer.CalcMaximalConnectCluster()
        #self.mrAnalyzer.CalcDelauNeis() #included in mrAnalyzer.calcMrQual
        self.mrAnalyzer.simplifiedClusterReport(clusterReportPath)
        self.logger.info('Maximal connected cluster calc & save completed.')
        self.storeClusterCalculationToDb()
        self.storeCellCoverageInfoToDb()


    def calcCellCoverageReportByScanData(self,iRxlevCell = 12,dRxlevCell = 4,aRxlevCell = 15,iRxlevSSI = 12,availTchCount= 67,SSIFilter = 1.0):
        '''CALC_CELL_COVERAGE_STAT'''
        self.logger.info('Start generating cell coverage report by scan data...')
        try:
            self.CellCoverageData = {}
            SSIresults = self.calcStreetStructualFeature(iRxlevSSI)
            ###dataSet[(sample['longitude'],sample['latitude'])] = ((sample['x'],sample['y']),trxCount,cellCount,(sample['projx'],sample['projy']))
            if not SSIresults:
                self.logger.error('Can not get street structural index result,reporting failed.')
                return False
            count = 0
            maxCount = len(SSIresults)
            for sampleCoords in SSIresults:
                streetIndex = SSIresults[sampleCoords][1]/float(availTchCount)
                ###sampleSelectSql = 'SELECT cgi,bcch,bsic,arxlev,samples,distance,ref_distance FROM raw_fmt_aggregate WHERE longitude = ? AND latitude = ? ORDER BY arxlev DESC'
                cellSamples = self.getFMTScanSampleByCoords(sampleCoords[0],sampleCoords[1])
                if not cellSamples or not cellSamples['samples']:
                    continue
                qualifiedNei = self.getDelnySurrondingCells((SSIresults[sampleCoords][0][0],SSIresults[sampleCoords][0][1]),[lsample[0] for lsample in cellSamples['samples'] if lsample[0]])
                if not qualifiedNei:
                    continue
                for spot in cellSamples['samples']:
                    if not spot[0]:
                        continue
                    if spot[0] not in self.CellCoverageData:
#                        CellCoverageData[spot[0]] = dict(
#                            validSamples = 0,cvs = 0,
#                            interferSamples = 0,cis = 0,
#                            dominateSamples = 0,cds = 0,
#                            overlappedInterferSamples = 0,cois = 0,
#                            overlappedDominateSamples = 0,cods = 0,
#                            sampleInBadSSI = 0,csibssi = 0,
#                            overlappedSampleInBadSSI = 0,cosibssi = 0
#                        )
                        self.CellCoverageData[spot[0]] = dict((item,0) for item in self.cellCoverageResultDictIndex)
                    if spot[3] >= aRxlevCell:
                        if spot[2] != "NULL":
                            self.CellCoverageData[spot[0]]['cvs'] +=1
                        self.CellCoverageData[spot[0]]['validSamples'] +=1
                    if cellSamples['maxRxlev'] - iRxlevCell <= spot[3] < cellSamples['maxRxlev'] - dRxlevCell:
                        if spot[2] != 'NULL':
                            self.CellCoverageData[spot[0]]['cis'] +=1
                        self.CellCoverageData[spot[0]]['interferSamples'] +=1
                    if spot[3] >= cellSamples['maxRxlev'] - dRxlevCell:
                        if spot[2] != 'NULL':
                            self.CellCoverageData[spot[0]]['cds'] +=1
                        self.CellCoverageData[spot[0]]['dominateSamples'] +=1
                    if cellSamples['maxRxlev'] - iRxlevCell <= spot[3] < cellSamples['maxRxlev'] - dRxlevCell and spot[0] not in qualifiedNei:
                        if spot[2] != "NULL":
                            self.CellCoverageData[spot[0]]['cois'] +=1
                        self.CellCoverageData[spot[0]]['overlappedInterferSamples'] +=1
                    if spot[3] >= cellSamples['maxRxlev'] - dRxlevCell and spot[0] not in qualifiedNei:
                        if spot[2] != "NULL":
                            self.CellCoverageData[spot[0]]['cods'] +=1
                        self.CellCoverageData[spot[0]]['overlappedDominateSamples'] +=1
                    if streetIndex >= SSIFilter and spot[3] >= cellSamples['maxRxlev'] - iRxlevSSI:
                        if spot[2] != "NULL":
                            self.CellCoverageData[spot[0]]['csibssi'] +=1
                        self.CellCoverageData[spot[0]]['sampleInBadSSI'] +=1
                    if streetIndex >= SSIFilter and spot[3] >= cellSamples['maxRxlev'] - iRxlevSSI and spot[0] not in qualifiedNei:
                        if spot[2] != "NULL":
                            self.CellCoverageData[spot[0]]['cosibssi'] +=1
                        self.CellCoverageData[spot[0]]['overlappedSampleInBadSSI'] +=1
                count += 1
                self.progressQueue.put(count*100/maxCount)

            self.logger.info('All calculation complete.')
            return self.CellCoverageData
        except:
            self.logger.exception('Unexpected error occured when generate cell coverage report.')
            return False
        finally:
            self.logger.info('Complete calc cell coverage stat by scan data.')

    def genCellCoverageReportByScanData(self,reportPath,iRxlevCell = 12,dRxlevCell = 4,aRxlevCell = 15,iRxlevSSI = 12,availTchCount= 67,SSIFilter = 1.0):
        self.logger.info('Start generating cell coverage report by scan data...')

        try:
            if not reportPath:
                self.logger.error('Missing saving path when generate cell coverage report')
                return False
            with open(reportPath,'wb') as fp:
                writer = csv.writer(fp)
                writer.writerow(['Inferior Rxlev for Interfer:{}'.format(iRxlevCell),])
                writer.writerow(['Inferior Rxlev for Dominate:{}'.format(iRxlevCell),])
                writer.writerow(['Absolute Rxlev for service:{}'.format(aRxlevCell),])
                writer.writerow(['Inferior Rxlev for SSI:{}'.format(iRxlevSSI),])
                writer.writerow(['Available TCH ARFCNs for SSI:{}'.format(availTchCount),])
                writer.writerow(['Critical SSI threshold:{}'.format(SSIFilter),])

                writer.writerow(['CGI',]+self.cellCoverageResultDictIndex)
                CellCoverageData = self.calcCellCoverageReportByScanData(iRxlevCell,dRxlevCell,aRxlevCell,iRxlevSSI,availTchCount,SSIFilter)
                if CellCoverageData:
                    for cgi in CellCoverageData:
                        writer.writerow([cgi,]+[CellCoverageData[cgi].get(item) for item in self.cellCoverageResultDictIndex])
                    self.logger.info('All generation complete.')
                    return True
                else:
                    self.logger.error('Can not get cell coverage statistics!')
                    return False
        except IOError:
            self.logger.exception('Unable save to file {}.'.format(reportPath))
            return False
        except:
            self.logger.exception('Unexpected error occured when generate cell coverage report.')
            return False
        finally:
            self.logger.info('Complete generating cell coverage report by scan data.')

    def getBlackMRList(self,minmumCells = 3 ,mrRatio = 0.1):
        self.logger.info('Fetch Black MR list started...')
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            cursor.execute('SELECT blackid,reportCells,bmrs,smrs,ratio FROM (SELECT blackid,count(scgi) AS reportCells,sum(blackmrs) AS bmrs,sum(servermrs) AS smrs,sum(blackmrs)/sum(servermrs) AS ratio FROM blackmr GROUP BY blackID) AS raw WHERE raw.reportCells >= ? AND raw.ratio >= ? ORDER BY reportCells DESC',(minmumCells,mrRatio))
            return cursor.fetchall()
        except:
            self.logger.exception('Unexpected error occured found when fetch Black MRs!')
        finally:
            self._releaseDBConn()
            self.logger.info('Fetch Black MR list completed.')

    def getBlackMrAffectCells(self,balckId,mrRatio = 0.1):
        dbConn = self._acquireDBConn(False)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            cursor = dbConn.cursor()
            cursor.execute('SELECT blackid,scgi,blackmrs as bmr,servermrs as smr,blackmrs / servermrs AS ratio FROM "blackMR" WHERE blackID = ? and ratio >= ?',(balckId,mrRatio))
            return cursor.fetchall()
        except:
            self.logger.exception('Unexpected error occured found when fetch Black MRs!')
        finally:
            self._releaseDBConn()

    def storeClusterCalculationToDb(self):
        dbConn = self._acquireDBConn(True)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            if self.mrAnalyzer.CorrelateMatrixBy12dB:
                self.logger.info('Cell correlation store started...')
                dbConn.execute('DROP TABLE IF EXISTS cellCorrelation')
                dbConn.execute(self.createCellCorrelationTableSql)
                dbConn.commit()
                cursor = dbConn.cursor()
                count = 0
                maxCount = len(self.mrAnalyzer.CorrelateMatrixBy12dB)
                for SCGI in self.mrAnalyzer.CorrelateMatrixBy12dB:
                    cache = []
                    for NCGI in self.mrAnalyzer.CorrelateMatrixBy12dB[SCGI]:
                        coe = self.mrAnalyzer.matchedMR.get(SCGI,{}).get(NCGI,{}).get('COR_COE')
                        if coe:
                            cache.append([SCGI,NCGI,coe])
                    #self.createCellCorrelationTableSql = 'CREATE TABLE IF NOT EXIST cellCorrelation (scgi TEXT,ncgi TEXT,coe REAL)'
                    cursor.executemany('INSERT INTO cellCorrelation (scgi,ncgi,coe) VALUES(?,?,?)',cache)
                    count += 1
                    self.progressQueue.put(count*100/maxCount)
                self.logger.info('Cell correlation stored.')
                dbConn.commit()
                cursor.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","cellCorrelation",datetime("now"))')
                dbConn.commit()
            else:
                self.logger.error('Please run cluster check first!')
        except:
            self.logger.exception('Unexpected error occured found when store Cell correlation to database!')
        finally:
            self._releaseDBConn()
            #self.logger.info('Store Cluster Calculation To Db done.')

        dbConn = self._acquireDBConn(True)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            if self.mrAnalyzer.CGI2HashedClusters:
                self.logger.info('Cell to cluster info store started...')
                dbConn.execute('DROP TABLE IF EXISTS cellToCluster')
                dbConn.execute(self.createCellToClusterTableSql)
                dbConn.commit()
                cursor = dbConn.cursor()
                count = 0
                maxCount = len(self.mrAnalyzer.CGI2HashedClusters)
                for CGI in self.mrAnalyzer.CGI2HashedClusters:
                    cursor.executemany('INSERT INTO cellToCluster (clusterid,cgi) VALUES (?,?)', [(hashid,CGI) for hashid in self.mrAnalyzer.CGI2HashedClusters[CGI]])
                    count += 1
                    self.progressQueue.put(count*100/maxCount)
                dbConn.commit()
                self.logger.info('Cell to cluster info store completed.')
                cursor.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","cellToCluster",datetime("now"))')
                dbConn.commit()
            else:
                self.logger.error('Please run cluster check first!')
        except:
            self.logger.exception('Unexpected error occured found when store Maximal connected cluster result to database!')
        finally:
            self._releaseDBConn()
            #self.logger.info('Store Cluster Calculation To Db done.')

    def storeCellCoverageInfoToDb(self):
        dbConn = self._acquireDBConn(True)
        if not dbConn:
            self.logger.error('Failed to obtain database connection,not exist or been used by others')
            return False
        try:
            self.logger.info('Cell interference info store started...')
            if not self.mrAnalyzer.isCellMrInfoCalced:
                self.mrAnalyzer.calcCellMrQual()
            dbConn.execute('DROP TABLE IF EXISTS cellIntfInfo')
            dbConn.execute(self.createCellIntfInfoTableSql)
            dbConn.commit()
            cursor = dbConn.cursor()
            count = 0
            maxCount = len(self.mrAnalyzer.CellCoverageInfo)
            for CGI in self.mrAnalyzer.CellCoverageInfo:
                try:
                    data = [CGI,
                            #self.mrAnalyzer.CellCoverageInfo[CGI].get('CoFreqAffectingOthers'),
                            #self.mrAnalyzer.CellCoverageInfo[CGI].get('CoFreqAffectedBy'),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('SrvCellInferiorTo12dBMRs'),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('SrvCellIntfToNeiMRsBy12dB'),
                            ','.join(self.mrAnalyzer.CellCoverageInfo[CGI].get('SrvCellOverlapedNeis')),
                            ','.join(self.mrAnalyzer.CellCoverageInfo[CGI].get('SrvCellOverlapedNeisBeyondDelauny')),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('SrvCellOverlapedTRXs'),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('SrvCellOverlapedTRXsBeyondDelNei'),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('SrvMRs'),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('TotalClusterCount'),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('CoArfcnCells'),
                            self.mrAnalyzer.CellCoverageInfo[CGI].get('CriticalClusterCount')
                    ]
                    cursor.execute('INSERT INTO cellIntfInfo (cgi,SrvCellInferiorTo12dBMRs,SrvCellIntfToNeiMRsBy12dB,SrvCellOverlapedNeis,SrvCellOverlapedNeisBeyondDelauny,SrvCellOverlapedTRXs,SrvCellOverlapedTRXsBeyondDelNei,SrvMRs,TotalClusterCount,CoArfcnCells,CriticalClusterCount) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       data )
                except sqlite3.InterfaceError:
                    self.logger.exception('Unexpected error occured when insert {}'.format(data))
                finally:
                    count += 1
                    self.progressQueue.put(count*100/maxCount)
            self.logger.info('Cell interference info store completed.')
            dbConn.commit()
            cursor.execute('INSERT INTO projectinfo (attribute,value,timestamp) VALUES("dataload","cellIntfInfo",datetime("now"))')
            dbConn.commit()
        except:
            self.logger.exception('Unexpected error occured found when store Cell Coverage Info To Db!')
        finally:
            self._releaseDBConn()
