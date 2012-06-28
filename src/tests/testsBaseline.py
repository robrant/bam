import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import unittest
import datetime
import random

# FOSS libs
import numpy as np

#Custom libs
import mdb
from timeSeriesDoc import timeSeries
from baseObjects import keyword as kw
import baselineProcessing as bl

class TestBaseline(unittest.TestCase):

    
    def testGetAllCountForOneCell(self):
        ''' Gets a count for a single cell'''
        
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections
 
        tweetTime = datetime.datetime(2011,1,2,12,5,15)
        oldTweetTime = tweetTime - datetime.timedelta(seconds=11*60)
 
        # Build a keyword to represent the basekine
        kword = kw(keyword='keyword1', timeStamp=oldTweetTime, lat=34.4, lon=45.5,
                           text='this text contained the hashtag #keyword1',
                           tweetID=346664, userID=4444, source='twitter')

        # New timeseries object
        ts = timeSeries()
        ts.importData(kword)
        success = ts.insertBlankDoc()

        # Build a keyword
        kword = kw(keyword='keyword1', timeStamp=tweetTime, lat=34.4, lon=45.5,
                           text='this text contained the hashtag #keyword1',
                           tweetID=346664, userID=4444, source='twitter')

        # New timeseries object
        ts = timeSeries()
        ts.importData(kword)
        success = ts.insertBlankDoc()


        # ALL DOCUMENTS
        mgrs    = '38SND4595706622'
        keyword = 'keyword1'
        
        # This indate represents when the baseline was run (12:10) minus the interest period (10 minutes)
        inDate = datetime.datetime(2011,1,2,12,0,0)
        results = bl.getResultsPerCell(dbh, collection='timeseries', mgrs=mgrs, keyword=keyword, inDate=inDate)

        self.assertEqual(len(results), 1)


    def testGetAllCountForOneCellLookback(self):
        ''' Gets a count for a single cell'''

       
        tweetTime = datetime.datetime(2011,1,2,12,5,15)
        oldTweetTime = tweetTime - datetime.timedelta(seconds=15*60)
        baselineTime = datetime.datetime(2011,1,2,12,0,0)
       
        # Get a db handle
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections
 
        # Build a keyword
        kword = kw(keyword='keyword1', timeStamp=tweetTime, lat=34.4, lon=45.5,
                           text='this text contained the hashtag #keyword1',
                           tweetID=346664, userID=4444, source='twitter')

        # New timeseries object
        ts = timeSeries()
        ts.importData(kword)
        success = ts.insertBlankDoc()
        
        # Last 2  documents
        lookback = 24
        mgrs    = '38SND4595706622'
        qKeyword = 'keyword1'
        res = bl.getResultsPerCell(dbh,
                                   collection='timeseries', 
                                   mgrs=mgrs, 
                                   keyword=qKeyword, 
                                   inDate=baselineTime,
                                   lookback=lookback)
        print res
        
        results = []
        for doc in res:
            print doc
            results.append(doc)
            
        self.assertEqual(len(results), 1)
        
        # Close the connection
        mdb.close(c, dbh)

    def testgetSliceTimeOfDay(self):
        ''' Gets a slice of each day preceding.'''
        
        # How big is the input array? 7 days long in this case...
        days, hours, minutes = 31, 24, 60
        
        # Build an array populated in certain hours
        arr = np.zeros((days,hours,minutes), np.int8)
        # For all days, hours 4 and 5 as 1 and flatten
        arr[:,14:16,:] = 1
        arr = arr.flatten()
        outArr = bl.getSliceTimeOfDay(arr, hoi=15, stepDays=7, pad=1)
        checkArr = np.ones((4, 120), np.int8)
        
        self.assertEquals(checkArr.tolist(), outArr.tolist())
        

    def testGetLookback(self):
        ''' Get a new datetime based on a lookback period.'''
        
        nowSurrogate = datetime.datetime(2012,1,2,12,6,3)
        lbh = 12
        
        # Truncating at day precision
        truth  = datetime.datetime(2012,1,1,23,59,0)
        start  = bl.getLookback(dt=nowSurrogate, lookbackHours=lbh, byDay=True)
        self.assertEquals(start, truth)
        
        # Not truncating at day precision 
        truth = datetime.datetime(2012,1,2,0,6,3)
        start = bl.getLookback(dt=nowSurrogate, lookbackHours=lbh)
        self.assertEquals(start, truth)
    
    
    def testBuildFullArray(self):
        '''Build a full array from a cursor result'''
        
        # Get a db handle
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections
 
        # Build a keyword
        kword = kw(keyword='keyword1', timeStamp=datetime.datetime(2011,1,2,12,1,1), lat=34.4, lon=45.5,
                           text='this text contained the hashtag #keyword1', tweetID=346664, userID=4444, source='twitter')

        # New timeseries object
        ts = timeSeries()
        ts.importData(kword)
        success = ts.insertBlankDoc()
        
        # Insert the doc now that its been modified
        kword.timeStamp = datetime.datetime(2011,1,1,12,1,1) 
        ts = timeSeries()
        ts.importData(kword)
        
        success = ts.insertBlankDoc()
        
        # Last 1 weeks worth of documents
        resultSet = bl.getResultsPerCell(dbh, '38SND4595706622', 'keyword1', datetime.datetime(2011,1,2), 168)
        
        # Inputs
        inDate = datetime.datetime(2011, 1, 2, 0, 0)
        period = datetime.timedelta(days=7)
        flat = None
        
        dates, data = bl.buildFullArray(resultSet, inDate, period, flat)
        
        self.assertEquals(len(dates), 8)
        self.assertEquals(len(data), 8)        

        # Close the connection
        mdb.close(c, dbh)


    def testBuildFullArrayFlat(self):
        '''Build a full FLATTENED array from a cursor result'''
        
        st = datetime.datetime.utcnow()
        
        # A keyword that went in yesterday creates a timeseries yesterday
        nowDt = datetime.datetime(year=2011,month=1,day=12,hour=11,minute=1,second=1)
        oneDay= datetime.timedelta(days=1)

        # Get a db handle
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections
        # Build a keyword
        kword = kw(keyword='keyword1', timeStamp=nowDt-oneDay, lat=34.4, lon=45.5,
                           text='this text contained the hashtag #keyword1',
                           tweetID=346664, userID=4444, source='twitter')
        # New timeseries object
        ts = timeSeries()
        ts.importData(kword)
        success = ts.insertBlankDoc()
        
        # Insert 2ND DOC IN THE COLLECTION
        kword.timeStamp = nowDt 
        ts = timeSeries()
        ts.importData(kword)
        success = ts.insertBlankDoc()
        
        nowDate = nowDt.replace(hour=0,minute=0,second=0,microsecond=0) 
        
        # Last 1 weeks worth of documents
        resultSet = bl.getResultsPerCell(dbh, '38SND4595706622', 'keyword1', nowDate, 168)
        # Close the connection
        mdb.close(c, dbh)

        # Inputs
        period = datetime.timedelta(days=7)
        dates, data = bl.buildFullArray(resultSet, nowDate, period, 1)
        
        
        firstDay = dates[0]
        lastDay = dates[-1]
        

        self.assertEquals(data.shape[0], 11520)
        self.assertEquals(firstDay, nowDate - period)
        self.assertEquals(lastDay, nowDate)
        
    
    def testlastBaselined(self):
        ''' Builds a baseline document for inserting.'''

        # Connect and get handle
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)
        
        # Build a keyword object
        testKywd = kw(keyword='keyword1',
                   timeStamp=datetime.datetime(2011,6,22,12,10,45),
                   lat=34.4, lon=45.5,
                   text='this text contained the hashtag #keyword1',
                   tweetID=346664, userID=4444, source='twitter')
        
        # Create a new baseline object
        baseLine = bl.baseline(kywd=testKywd, cellBuildPeriod=600)
        
        baseLine.outputs['days30_all']      = 0.5
        baseLine.outputs['days7_all']       = 0.4
        baseLine.outputs['hrs30_all']       = 0.3
        baseLine.outputs['days30_weekly']   = 0.2
        baseLine.outputs['days7_daily']     = 0.1
        
        doc = baseLine.buildDoc()
        bl.insertBaselineDoc(dbh, doc)
        
        # Method returns the date of last baseline calculation
        lastBaseline = baseLine.lastBaselined()
        self.assertEquals(lastBaseline, datetime.datetime(2011,6,22,12,10))

        # Close the connection
        mdb.close(c, dbh)


    def testInsertBaselineDoc(self):
        ''' Inserts a completed baseline document into the baseline collection.'''
        
        # Connect and get handle
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)

        # Build a keyword object
        testKywd = kw(keyword='keyword1',
                   timeStamp=datetime.datetime(2011,6,22,12,10,45),
                   lat=34.4, lon=45.5,
                   text='this text contained the hashtag #keyword1',
                   tweetID=346664, userID=4444, source='twitter')
        
        # Instantiate the baseline object/class
        baseLine = bl.baseline(kywd=testKywd,cellBuildPeriod=600)

        # Build the document and insert it
        doc = baseLine.buildDoc()
        bl.insertBaselineDoc(dbh, doc)
        
        res = dbh.baseline.find()[0]
        print res
        
        self.assertEquals(res['keyword'], 'keyword1')
        self.assertEquals(res['mgrs'], '38SND4595706622')
        self.assertEquals(res['mgrsPrecision'], 10)

        # Close the connection
        mdb.close(c, dbh)

        

    '''
        TESTS TO CHECK:
        1. Accurate population of days30_all           /
        2. Accurate population of days7_all            /
        3. Accurate population of hrs30_all            /
        4. Accurate population of days30_weekly        /
        5. Accurate population of days7_daily          /
    '''

    def testjustZeros(self):
        '''    '''
        
        inArr = np.zeros((10), np.int8)
        just0 = bl.justZeros(inArr)
        self.assertTrue(just0)
        
        inArr = np.zeros((10), np.int8)
        inArr[5] = 1
        just1 = bl.justZeros(inArr)
        self.assertFalse(just1)
                
        inArr = np.zeros((10), np.int8)
        inArr[5] = 40
        just1 = bl.justZeros(inArr)
        self.assertFalse(just1)

    
    def testProcessBaselineLast30Days(self):
        ''' Checks accurate population of an array for 30 day all '''
        
        # Connect and get handle
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)

        # Set up some times to work with
        tweetTime = datetime.datetime.utcnow()
        thisMinute = tweetTime.replace(second=0,microsecond=0)
        today = tweetTime.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Thirty days ago - at the start of the day
        lastMonthTweet = tweetTime - datetime.timedelta(days=30)
        
        # Build a keyword object
        testKywd = kw(keyword='keyword1',
                      timeStamp=lastMonthTweet,
                      lat=34.4, lon=45.5,
                      text='this text contained the hashtag #keyword1',
                      tweetID=346664, userID=4444, source='twitter')
        
        # Insert a new timeseries object for the tweet 30 days ago
        ts = timeSeries()
        ts.importData(testKywd)
        success = ts.insertBlankDoc()
        ts.updateCount()
        
        # Create a keyword object for the current tweet
        testKywd2 = kw(keyword='keyword1',
                       timeStamp=lastMonthTweet + datetime.timedelta(hours=1),
                       lat=34.4, lon=45.5,
                       text='this text contained the hashtag #keyword1',
                       tweetID=346664, userID=4444, source='twitter')
        
        # Insert the current keyword - NOTE HOW THIS IS AFTER THE BASELINE BUILD
        ts2 = timeSeries()
        ts2.importData(testKywd2)
        success = ts2.insertBlankDoc()
        ts2.updateCount()
        
        # Create a keyword object for the current tweet
        testKywd3 = testKywd
        testKywd3.timeStamp = tweetTime
        # Instantiate the baseline object/class
        base = bl.baseline(kywd=testKywd3, cellBuildPeriod=600)
        if base.needUpdate == True:
            if not base.lastBaselined():
                doc = base.buildDoc()
                bl.insertBaselineDoc(dbh, doc)
        
        # Insert the current keyword - NOTE HOW THIS IS AFTER THE BASELINE BUILD
        ts3 = timeSeries()
        ts3.importData(testKywd3)
        success = ts3.insertBlankDoc()
        ts3.updateCount()

        tweetTimeMinus2Days = tweetTime - datetime.timedelta(days=2)
        
        # Create a new keyword object to test the daily slicing
        testKywd5 = kw(keyword='keyword1',
                       timeStamp=tweetTimeMinus2Days,
                       lat=34.4, lon=45.5,
                       text='this text contained the hashtag #keyword1',
                       tweetID=346664, userID=4444, source='twitter')
        
        # Insert the current keyword - NOTE HOW THIS IS AFTER THE BASELINE BUILD
        ts5 = timeSeries()
        ts5.importData(testKywd5)
        success = ts5.insertBlankDoc()
        ts5.updateCount()

        # Process Baseline
        base.processBaseline()
        
        # Get back the 30 day array
        arr = base.test30DayArray
        
        # Calculate what the array length should be
        soFarToday = (thisMinute - today).seconds/60.0
        
        # The start of the array datetime
        lastMonthDay = lastMonthTweet.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # The number of days between today and the start of the array (then in minutes)
        dateDiff = (today - lastMonthDay)
        minsDiff = dateDiff.days*1440 + dateDiff.seconds/60.0 
        total = minsDiff + soFarToday
        
        # Confirm its the right length
        self.assertEqual(total, len(arr))
        
        # Get the minutes for the first 2 keywords (the third shouldn't be there)
        kwd1Min = int((testKywd.timeStamp - lastMonthDay).seconds/60)
        kwd2Min = int((testKywd2.timeStamp - lastMonthDay).seconds/60)
        
        kwd1Test = [arr[kwd1Min-1], arr[kwd1Min], arr[kwd1Min+1]]
        kwd2Test = [arr[kwd2Min-1], arr[kwd2Min], arr[kwd2Min+1]]
        
        for j in arr:
            if arr[j] > 0:
                print j, arr[j]
        
        self.assertEquals(kwd1Test, [0,1,0])
        self.assertEquals(kwd2Test, [0,1,0])
        
        # 30 DAY TIME SLICE CHECK
        arr = base.test30DaySliced
        # weekly 
        testSliced = int(30/7) * 6 * 60
        self.assertEquals(testSliced, len(arr))
        
        arr7Day = base.test7DayArray
        test7DayAll = (thisMinute - today).seconds/60.0 + 1440*7
        self.assertEquals(len(arr7Day), int(test7DayAll))
        
        arr30Hrs = base.test30hrArray
        test30Hours = 30*60
        self.assertEquals(len(arr30Hrs), int(test30Hours))
        
        # Close the connection
        mdb.close(c, dbh)

    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()