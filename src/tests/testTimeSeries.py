
import sys

importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

# Standard libraries
import unittest
import datetime
import random

# Custom libs
from timeSeriesDoc import timeSeries
from baseObjects import keyword
import mdb

class TestTimeSeries(unittest.TestCase):
    
    def setUp(self):
        ''' Set up a keyword object to use '''
        
        ts       = datetime.datetime(2011,1,2,12,1,1)
        lat, lon = 34.4, 45.5
        tx1      = 'this text contained the hashtag #keyword1'
        kw1      = 'keyword1'
        tId      = 346664
        uId      = 4444
        source   = 'twitter'
        
        # Build a keyword object for use in testing timeSeries
        self.kw = keyword(keyword=kw1, timeStamp=ts, lat=lat, lon=lon, text=tx1,
                          tweetID=tId, userID=uId, source=source)
        print self.kw.mgrs


    def testGetStart(self):
        ''' Checks getting the start dt '''
        
        # A days worth of hour 
        #times = list(datetime.datetime(2011,1,1, i, random.randint(0,59)) for i in range(0,24,4))
        inTime = datetime.datetime(2011,1,1,7) 
        
        blockPrecisions = [
                           [1,  datetime.datetime(2011,1,1,7)],
                           [4,  datetime.datetime(2011,1,1,4)],
                           [6,  datetime.datetime(2011,1,1,6)],
                           [12, datetime.datetime(2011,1,1,0)],
                           [24, datetime.datetime(2011,1,1,0)]
                           ]

        # Test the input block precision against the output dt        
        for bp in blockPrecisions:  
            ts = timeSeries()
            start = ts.getStart(timeStamp=inTime, blockPrecision=bp[0])
            self.assertEquals(start, bp[1])
            
            # Disconnect
            ts.c.disconnect()
        
    
    def testImportData(self):
        ''' Check import of keyword object.'''
        
        # New timeseries object
        ts = timeSeries()
        
        # Import the data
        ts.importData(self.kw)
        
        print 'start',ts.start
        self.assertEquals(ts.start, datetime.datetime(2011,1,2,0,0,0))
        self.assertEquals(ts.source, 'twitter')
        self.assertEquals(ts.period, datetime.timedelta(hours=24))
        self.assertEquals(ts.mgrs, '38SND4595706622')
        
        # Close the connection
        ts.c.disconnect()

    def InsertBlankDoc(self):
        ''' Checks the successful inserting of a mongo document '''
        
        # Get connection to mongo
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections        
        
        # New timeseries object with data
        ts = timeSeries()
        ts.importData(self.kw, blockPrecision=1)

        # Build and insert a new mongo formatted document
        success = ts.insertBlankDoc()
        self.assertEquals(success, 1)
        
        # Clean up and drop it
        #dbh.timeseries.remove()
        
        # Close the connection
        mdb.close(c, dbh)

    
    def testUpdateDocument(self):
        ''' Function updates/increments a specific hour.minute in a document.   '''

        # Get connection to mongo
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections        
       
        # New timeseries object with data
        ts = timeSeries()
        ts.importData(self.kw, blockPrecision=24)

        success = ts.insertBlankDoc()
        self.assertEquals(success, 1)

        # Update/increment a specific hour.minute
        ts.updateCount()

        # Run a query for this item
        outDocs = dbh.timeseries.find({'data.12.1':1})

        for doc in outDocs:
            print doc
            self.assertEquals(doc['mgrs'], '38SND4595706622')

        # Close the connection
        mdb.close(c, dbh)
    
    
    def MongoLookup(self):
        ''' Fn checks whether a timeseries document already exists for this period.   '''

        # Get connection to mongo
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)         # Set up collections        
        
        # New timeseries object with data
        ts = timeSeries()
        ts.importData(self.kw, blockPrecision=1)

        # Check the count - should be 0 before the doc gets inserted
        count = ts.mongoLookup()
        self.assertEquals(count, 0)
        
        # Build and insert a new mongo formatted document
        success = ts.insertBlankDoc()
        
        # Count should be 1 now that the document has been inserted
        count = ts.mongoLookup()
        self.assertEquals(count, 1)
        
        # Clean up, remove he content and close the connection
        #dbh.timeseries.remove()
        mdb.close(c, dbh)
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test']
    unittest.main()