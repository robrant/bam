import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

# Standard libs
import datetime
import re

# Non-standard FOSS lib
import numpy as np
#Custom libs
from baseObjects import keyword
import mdb
from pymongo import objectid

#----------------------------------------------------------------------------------------------------      

def buildBlankData(hours=24, mins=60):
    ''' Builds an attribute representing a day (default). Might want to have the
        array start life as a flat (1D) array and then resize it. This would be 
        consistent with the output format - a flat 1D array. ***NOT TESTED*** '''
    
    # A blank numpy array that is converted into a python list
    arr = np.zeros((hours,mins), np.int)
    d = arr.tolist()
    return d

#----------------------------------------------------------------------------------------------------      

def buildObjectId(dtg, mgrs, keyword):
    '''Builds a simple object Id for quick duplication checking.'''
    
    # Format the datetime
    strDtg = dtg.strftime("%d%m%y")
    
    # Kludge it all together
    out = "%s%s%s" %(strDtg, keyword, mgrs)
    
    return out

#----------------------------------------------------------------------------------------------------      

class timeSeries():
    
    def __init__(self, c=None, dbh=None, host=None, port=None, db=None):
        ''' instantiate the object and attributes. ***NOT TESTED*** '''

        if not c and not dbh:
            # Get a connection to the db    
            self.c, self.dbh = mdb.getHandle(host=host, port=port, db=db)
        else:
            self.c, self.dbh = c, dbh
        
#------------------------------------------------------------------------------------------
    
    def importData(self, kw, blockPrecision=24):
        ''' Import data into the object. Tested.'''
        
        # Re-assign the time series, source and keyword
        self.timeStamp = kw.timeStamp
        self.source    = kw.source
        self.keyword   = kw.keyword
        
        # The start date of the time series
        self.start = self.getStart(self.timeStamp, blockPrecision)
        
        # The length of time that the object represents
        self.period = datetime.timedelta(hours=blockPrecision)
        self.blockPrecision = blockPrecision
        
        # Re-assign geospatial information (mgrs, lat, lon)
        self.mgrs           = kw.mgrs
        self.mgrsPrecision  = kw.mgrsPrecision
        self.lat, self.lon  = kw.lat, kw.lon
        
        # Build the object Id
        #self._id = buildObjectId(self.timeStamp, self.mgrs, self.keyword)
    
#----------------------------------------------------------------------------------------------------
    
    def getStart(self, timeStamp, blockPrecision):
        ''' Sets the start datetime for the storage block. 
            This is NOT THE PRECISION OF THE density slices - its of the timeseries
            block that will store the density information for a period.
            COULD DO WITH A CHECK TO MAKE SURE THE BP IS CLEANLY DIVISABLE BY 24.
            Tested.'''

        # Calculate the truncated hour at this precision
        hour = int(np.floor(float(timeStamp.hour)/float(blockPrecision))*blockPrecision)         
        
        # Assign that to the start attribute
        start = datetime.datetime(timeStamp.year, timeStamp.month, timeStamp.day, hour)

        return start
        
#----------------------------------------------------------------------------------------------------
        
    def mongoLookup(self):
        ''' Checks for the timeseries in mongo already. Might run into difficulties with
            threading with this - simultaneous creation of time series events for this keyword.
            TESTED.'''

        # Query parameters - query by: start, keyword, mgrs, and mgrs precision
        query = {"mgrs"    : self.mgrs,
                 "keyword" : self.keyword,
                 "start"   : self.start}
         
        projection = {"mgrs"    : 1,
                      "keyword" : 1,
                      "start"   : 1,
                      "_id"     : 0}
        
        return self.dbh.timeseries.find(query, projection).count()
        
#----------------------------------------------------------------------------------------------------

    def insertBlankDoc(self, blankData):
        ''' Builds a mongo document and inserts it. TESTED '''
        
        # Check to see if there is already a TS doc for this day
        if self.mongoLookup() == 1:
            return None
        
        doc = {'mgrs'           : self.mgrs,
               'mgrs_precision' : self.mgrsPrecision,
               'keyword'        : self.keyword,
               'source'         : self.source,
               'start'          : self.start,
               #'data'           : self.buildBlankData(hours=int(self.blockPrecision)),    # SPEED MODIFICATION
               'data'           : blankData
               }
            
        try:
            self.dbh.timeseries.insert(doc)
            return 1
        except:
            print "Failed to insert object into mongo 'timeseries' collection."
            return None

#----------------------------------------------------------------------------------------------------      
    
    def updateCount(self, incrementBy=1):
        ''' Assuming there is already a time series document available in the
         collection for this mgrs/keyword/period, this fn updates the relevant cell
          ***NOT TESTED***
          db.timeseries.update({"foo":"a"},{"$inc":{"visits.minutes.0.2":5}});
        '''

        # Find the right document
        query = {"mgrs":    self.mgrs,
                 "start":   self.start,
                 "keyword": self.keyword}
        
        # The update command to run
        toUpdate = "data.%s.%s" %(self.timeStamp.hour, self.timeStamp.minute)
        command = {"$inc": {toUpdate:incrementBy}}
        
        # Conduct the update
        try:
            self.dbh.timeseries.update(query, command)
        except:
            print 'Failed to update the time series.'
        
        # IS THERE A BETTER WAY OF CATCHING THIS ERROR?

#----------------------------------------------------------------------------------------------------      


            
            
            
            
        