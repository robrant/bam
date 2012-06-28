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
import numpy.ma as ma

#Custom libs
import mdb
import timeSeriesDoc as tsd
from baseObjects import keyword as kw

# Calling a normal/std function for the baseline statistic generation
from calculateBaseline import normalStd as baselineStatistic

"""
The baseline module contains functions for updating the cell-keyword baselines, 
against which current values are compared to find anomalies. 

To Do:
------

Main - Coordinates the retrieval of different anomaly levels (all, 24hrs, 1wk, 1month)


$$* LAST 24 HOURS: Retrieve last 30 hours of data into an array. Queries for the last 3 consecutive
   documents for the KEYWORD and MGRS (NEED ALSO TO THINK ABOUT ADJACENT CELLS?)

$$* SAME TIME FOR LAST 7 DAYS: Retrieves the last weeks worth of data and cuts out
  sections that are either side of the current time of day. For a tweet at 6pm, it
  would retrieve a list of arrays that have data between 4 and 8pm. Each processed
  in turn.

$$* EVERYTHING: Retrieve a count of any documents for this KEYWORD-MGRS in the complete dataset.
   If this is 0, then none of the subsequent queries are necessary.
   

* GENERAL FUNCTION: Function to retrieve the cells adjacent to the current one - args: number of cell buffer.
   Returns the MGRS of those cells surrounding the one provided. Accounts for the
   MGRS precision as well. This function will allow baselines to be computed based
   not just on a single cell, but on a wider area.
   
$$* GENERAL FUNCTION: Function to submit a query and return a cursor (contains all the error catching in it).
   This gets called by all the other functions getting data.

$$* GENERAL FUNCTION: Unpacks the time series data into a numpy array. Handles the absence of documents
   and fills them in as necessary. Uses a template for the absent days.

$$* LAST WEEK: Retrieves x-hours worth of data either side of 7 days ago. Defaults to 12 hours either
   side of the current time.

$$* LAST MONTH: Retrieves 2 days either side of the current time 1 month previous. Returns
  an array for processing.
  
* BUILD THE BASELINE: Provided with an array to process, it computes a baseline value. This
  is returned. 
  
* COMPILE BASELINE DOC: Function is called by main and builds a mongo doc for modifying
  in the main(). As a baseline val is returned for each period, its chucked into this
  document
  
* INSERT THIS BASELINE DOC: The document that has been built up is inserted.

  
REALLY NEED TO CHECK THAT THE DIFFERENT CONFIGURATIONS OF BASELINES DO NOT CONTAIN THE CURRENT VALUE
OTHERWISE THIS WILL SKEW THE RESULTS BADLY.

THEN NEED TO STITCH TOGETHER THESE SEPARATE FUNCTIONS INTO A SCRIPT. THIS WILL BUILD A BASELINE
OBJECT, THEN AS IT GETS THE DIFFERENT BASELINES, ASSIGN THEM TO THE OBJECT. IF THE COUNT OF 
EVERYTHING IS 0, IT CAN SKIP ALL THE OTHERS.

IF THERE HAS BEEN DATA PREVIOUSLY, THEN IT'LL NEED TO DO ALL OF THE BASELINES AND ASSIGN EACH ONE
TO THE OBJECT.

ONCE ALL THATS DONE, CALL buildDoc() ON THE OBJECT AND THEN INSERT THE DOC INTO MONGO.


THEN NEED TO WORK HOW/WHEN THIS SCRIPT (main()) IS CALLED. MIGHT BE SOME TEXT ABOUT THIS FURTHER UP.
SOMETHING LIKE:

1. After time series has been built for today.
2. Before the value for this minute gets added in, query baseline collection for baseline docs of this mgrs/kw.
3. If the modified datetime is earlier than the current minute, then call baseline.main()

4. At the end of the minute, process all those keyword/mgrs' in baseline collection that have 
   a modified datetime of the current minute.



"""



#----------------------------------------------------------------------------------------------------

def upsertBaselineDoc(dbh, doc, outputs, collection='baseline'):
    ''' Builds a mongo document and inserts it.
    NOT TESTED '''
    
    cHandle = dbh[collection]

    # Define the query - what doc do you want to upsert?
    q = {'mgrs'          : doc['mgrs'],
         'keyword'       : doc['keyword']}
    
    # Define the parameters to change?
    setDict = {'timeStamp':doc['timeStamp']}
    
    for key in outputs:
        setDict[key] = outputs[key]
        u = {'$set':setDict}
    
    response = cHandle.update(q, u, True)
    
    return response

#----------------------------------------------------------------------------------------------------
    
def truncateTimeStamp(timeStamp, unit, precision):
    ''' Truncates the timestamp to a certain precision'''
    
    if unit == 'hour':
        val = timeStamp.hour
    elif unit == 'minute':
        val = timeStamp.minute
    elif unit == 'second':
        val = timeStamp.second
    
    while val % int(precision) != 0:
        val -= 1
    
    if unit == 'hour':
        timeStamp = timeStamp.replace(hour=val, minute=0, second=0, microsecond=0)
    elif unit == 'minute':
        timeStamp = timeStamp.replace(minute=val, second=0, microsecond=0)
    elif unit == 'second':
        timeStamp = timeStamp.replace(second=val, microsecond=0)

    return timeStamp

#----------------------------------------------------------------------------------------------------

#def buildBlankData(hours=24, mins=60):
#    ''' Builds an attribute representing a day (default). Might want to have the
#        array start life as a flat (1D) array and then resize it. This would be 
#        consistent with the output format - a flat 1D array. ***NOT TESTED*** '''
#    
#    # A blank numpy array that is converted into a python list
#    arr = np.zeros((hours,mins), np.int)
#    return arr.tolist()

#----------------------------------------------------------------------------------------------------

def buildFullArray(resultSet, inDate, period, blankDay, flat=None):
    ''' Builds a date ordered array for the period specified, populated with 
        either 24x60 arrays from mongo (ie. with content) or 24x60 array without.
        Returns either a 3D array (day x hours x minutes) or a flattened 1D array.
        Also returns the list of python datetime.datetimes for the array.
        TESTED.'''

    # Check that input period is a timedelta object.
    #assert isinstance(period, datetime.timedelta)==True

    # Get the start date
    inDay = inDate.replace(hour=0,minute=0,second=0,microsecond=0)
    startDate = inDay - period
    
    # Get the results out of the cursor and into a python list
    #results  = [res for res in resultSet]
    ## blankDay = buildBlankData(hours=24, mins=60)    PULL THIS IN DIRECTLY FOR SPEED IMPROVEMENT

    # Build a list of days that should be in the resultSet
    docDates, dateList = [], []
    for d in range(period.days+1):
        
        # Flag for whether a document exists in mongo for this date
        mongoData = None
        docDate = startDate + datetime.timedelta(days=d)
        dateList.append(docDate)
        
        # Loop the mongo results to see if this date is in there
        for result in resultSet:
            # If a document was built for this day - append the data
            if result['start'] == docDate:
                docDates.append(result['data'])
                mongoData = 1
            
        # If no mongo timeseries exists, append a blank day document    
        if not mongoData:           
            docDates.append(blankDay)

    # Convert to numpy array and then flatten if required
    outArr = np.array(docDates, np.int8)
    if flat:
        outArr   = outArr.flatten()
    
    return dateList, outArr

#----------------------------------------------------------------------------------------------------

def getLookback(lookbackHours, dt=None, byDay=None):
    ''' Returns a datetime which represents a 'query from' time (used with $gt) based 
        on day-based timeseries documents.
        TESTED.'''

    # Check that input datetime is a datetime.datetime obj.
    if not dt and isinstance(dt, datetime.datetime)==False:
        dt = datetime.datetime.utcnow()
        
    ql = datetime.timedelta(hours=float(lookbackHours))
    qStart = dt - ql

    # This to get the query start time into the preceding day
    if byDay:
        qStart = qStart.replace(hour=0, minute=0, second=0, microsecond=0)
        shift = datetime.timedelta(minutes=1)
        qStart = qStart - shift
        
    return qStart

#----------------------------------------------------------------------------------------------------

def getResultsPerCell(dbh, collection, mgrs, keyword, inDate, lookback=None, countOnly=None):
    ''' Gets documents for the MGRS and keyword. Lookback is in hours from the current UTC time or supplied
        inDate. countOnly will return just the count. countOnly=None will return the documents.
        TESTED. 
        inDate is the end of the baseline period. It is the time at which the baseline is run 
        minus the current interest period. If the baseline was run at 12:10 and the period of interest
        was 10 minutes, this value would be 12:00:00.'''

    # Query by mgrs and keyword as a minimum
    query = {"mgrs"    : mgrs,
             "keyword" : keyword}
    
    # For retrieving a specified period prior to UTC now
    if lookback:
        gtDate = getLookback(lookback, dt=inDate, byDay=1)
        query['start'] = {'$gte': gtDate}
        
        # Get the start of the cell build/alert build period (so that none of the data to be compared is in the baseline)
        query['start'] = {'$lt': inDate}
        
    projection = {"_id" :     0,
                  "mgrs":     1,
                  "keyword" : 1,
                  "start":    1}
    
    # Request for count will return a count - otherwise a cursor
    if countOnly:
        res = dbh[collection].find(query, projection).count()
    else:
        res = dbh[collection].find(query)
    
    return [doc for doc in res]

#----------------------------------------------------------------------------------------------------

def getSliceTimeOfDay(arr, hoi=15, stepDays=1, stepHours=24, stepMins=60, pad=3, flat=None):
    ''' Takes in flattened array, a time of interest and a number of hours
        by which to pad the toi. From this, it builds a subset array of just the hours of interest.
        TESTED. '''
    
    # Check that the timeseries is an array and that the toi is a datetime.time obj
    assert isinstance(arr, np.ndarray)
    assert len(arr.shape)==1
    
    # The size of each step size in the loop
    stepInMins = stepDays * stepHours * stepMins
    steps = int(arr.size/(stepInMins)) 
    
    # Define the output array
    outArr = np.zeros((steps, pad*60*2), np.int8)

    # Loop through the array by steps, get start/end of segment to cut out
    # and populate a new numpy array of the periods of interest either side
    # of the hour of interest (hoi).    
    for i in range(steps):
        
        hoiIdx      = hoi*60 + stepInMins*i
        startIdx    = hoiIdx - pad * 60
        endIdx      = hoiIdx + pad * 60

        if startIdx < 0:
            addOn = np.zeros((abs(startIdx)), np.int8)
            startIdx = 0
            outArr[i,:] = np.hstack((addOn, arr[startIdx:endIdx]))

        elif endIdx > arr.size:
            addOn = np.zeros((endIdx - arr.size), np.int8)
            endIdx = arr.size
            outArr[i,:] = np.hstack((arr[startIdx:endIdx], addOn))

        else:
            outArr[i,:] = arr[startIdx:endIdx]
            
        
    
    # If the array is to be flattened
    if flat:
        outArr   = outArr.flatten()        
    
    return outArr
    
#----------------------------------------------------------------------------------------------------
"""
def getCellCluster(dbh, mgrs, mgrsPrecision, buffer):
    ''' Builds a list of those cells surrounding the current one of interest.
        NOT TESTED. '''

    # Split the MGRS up
    prec = mgrsPrecision / 2
    
    regExp = re.compile(r'^(\d{2})(\w{1})(\d{%s})(\d{%s})' %(prec, prec))
    mgrsSplit   = re.findall(regExp, mgrs)
    
    reg, des, xMgrs, yMgrs = mgrsSplit
    xMgrs, yMgrs = int(xMgrs), int(yMgrs)
    
    # Holds the mgrs cells that form the cluster
    cluster = []
    
    st  = -1 * buffer
    sp  = buffer
    
    # Loop elements of the buffered cluster and get equivalent MGRS cells
    for x in range(st, sp):
        print x
        for y in range(st, sp):
            print y
            
            # Get MGRS x and y
            xCell = xMgrs + x
            yCell = yMgrs + y
            
            # Deal with boundaries between MGRS areas


#----------------------------------------------------------------------------------------------------

def getAllForCellCluster(dbh, keyword):
    ''' '''

    # Query parameters - query by: start, keyword, mgrs, and mgrs precision
    q = {"mgrs"             : keyword.mgrs,
         "keyword"          : keyword.keyword,
         "start"            : keyword.start,
         'mgrs_precision'   : keyword.mgrsPrecision}
     
    count = dbh.timeseries.find(q).count()
    
    return count
"""
#---------------------------------------------------------------------------------------------------------#

def justZeros(inArr):
    ''' Checks to see whether its just an array of zeros. TESTED. '''

    # Build a replica of zeros
    testArr = np.zeros((len(inArr)), inArr.dtype)
    
    if testArr.any() == inArr.any():
        justZeros = True
    else:
        justZeros = False
    
    return justZeros

#=======================================================================================================

class baseline(object):


    def __init__(self, kywd, c=None, dbh=None, host=None, port=None, db=None, baselinePrecision=[]):
        ''' Constructor. '''
        
        # Connection for this baseline class
        if not c and not dbh:
            # Get a connection to the db    
            self.c, self.dbh = mdb.getHandle(host=host, port=port, db=db)
        else:
            self.c, self.dbh = c, dbh
            
        self.collection = 'timeseries'
        
        # Bad, bad version of inheriting keyword - NEED TO FIX THIS
        self.keyword        = kywd.keyword
        self.timeStamp      = kywd.timeStamp
        self.mgrs           = kywd.mgrs
        self.mgrsPrecision  = kywd.mgrsPrecision
        
        # Has this keyword ever been seen before at this location?
        self.outputs     = {}
        
        # When was the last baseline processed (units = hour or minute or second; precision = int val)
        # Or... what was the most recent baseline run - parameters come from top level
        unit, precision = baselinePrecision
        
        self.baselineTimeStamp = truncateTimeStamp(self.timeStamp, unit, precision)
        
        # Checks to see whether it exists at all or if it needs updating.
        # Outside the object, this is used to decide whether to continue with the processing      
        self.needUpdate = self.needUpdating()

    #----------------------------------------------------------------------------------------------------
        
    def getThisMinute(self, timeStamp, cellBuildPeriod):
        ''' Gets the timestamp for this minute - the cell building period.
            cellBuildPeriod is in seconds.
            NOT TESTED.'''
        
        # Truncate to the nearest minute
        timeStamp = timeStamp.replace(second=0, microsecond=0)
        
        # Now subtract the period over which the cell will be built
        timeStamp - timeStamp - datetime.timedelta(cellBuildPeriod)

        return timeStamp
        
    #----------------------------------------------------------------------------------------------------
        
    def needUpdating(self):
        ''' Handles the question - is there a baseline built and if so, does it need updating?. 
            NOT TEST.'''
        
        update = True
        
        # Get the last baselined date
        lastBaseline = self.lastBaselined()
        
        # If a baseline was built, check its date against the current minute
        if lastBaseline:
            if lastBaseline >= self.baselineTimeStamp:
                update = False

        return update
        
    #----------------------------------------------------------------------------------------------------
        
    def lastBaselined(self):
        ''' Checks when the last baseline was built for this time. UPDATE TEST.'''
        
        # Build a query based on the attributes of this class
        query = {'keyword' : self.keyword,
                 'mgrs'    : self.mgrs}
        
        projection = {'_id'       : 0,
                      'keyword'   : 1,
                      'mgrs'      : 1,
                      'timeStamp' : 1}
        
        docs = self.dbh.baseline.find(query, projection).limit(1)
        doc = [d for d in docs]
        if doc:
            timeStamp = doc[0]['timeStamp']
        else:
            timeStamp = None            

        return timeStamp
        
    #----------------------------------------------------------------------------------------------------
        
    def buildDoc(self):
        ''' Builds the document for inserting into mongo'''
        
        doc = {'keyword'       : self.keyword,
               'timeStamp'     : self.baselineTimeStamp,
               'mgrs'          : self.mgrs,
               'mgrsPrecision' : self.mgrsPrecision}
        
        # Add the outputs to the mongo doc if the 
        
        # COMMENTED OUT TO TRY THE UPSERT METHOD.
        
        #if self.outputs != {}:
        #    for key in self.outputs:
        #        doc[key] = self.outputs[key]
        

        return doc

    #----------------------------------------------------------------------------------------------------
    
    def processBaseline(self, blankData):
        ''' Coordinates the building of baseline values. Assumes that a baseline object
            already exists - where the creation is done by externally calling buildDoc().'''

        # Assign Default values to the output dictionary elements
        self.outputs['days30_all']      = 0
        self.outputs['days7_all']       = 0
        self.outputs['hrs30_all']       = 0
        self.outputs['days30_weekly']   = 0
        self.outputs['days7_daily']     = 0
        
        # PREVIOUS 30 DAYS = 720 hours
        results = getResultsPerCell(self.dbh, self.collection, self.mgrs, self.keyword, self.baselineTimeStamp, lookback=720)
        
        # If there weren't any documents in the lookback period, everything to 0
        if len(results) != 0:
            
            period = datetime.timedelta(days=30)
            dateList, outArr = buildFullArray(results, self.baselineTimeStamp, period=period, blankDay=blankData, flat=1)
            
            # How many minutes make up the day up til tweet time?
            dayTimeStamp    = self.baselineTimeStamp.replace(hour=0, minute=0, second=0, microsecond=0)
            nowMinutes      = int((self.baselineTimeStamp - dayTimeStamp).seconds / 60)

            # Minutes so far today + minutes for the last x(30) days
            arrCut = (period.days * 1440) + nowMinutes
            last30DayArr    = outArr[:arrCut]
            
            # ========= TESTING ONLY ========
            self.test30DayArray = last30DayArr
            # ========= TESTING ONLY ========
            
            # Calculate the baseline value
            self.outputs['days30_all'] = baselineStatistic(last30DayArr)
            
            # Slice out the hours of interest and calculate a different baseline value
            slicedArr = getSliceTimeOfDay(last30DayArr, stepDays=7, hoi=self.baselineTimeStamp.hour, pad=3, flat=1)
            
            # ========= TESTING ONLY ========
            self.test30DaySliced = slicedArr
            # ========= TESTING ONLY ========
            
            self.outputs['days30_weekly'] = baselineStatistic(slicedArr)
            
            # PREVIOUS 7 DAYS
            last7DayArr = last30DayArr[33120:]

            # ========= TESTING ONLY ========
            self.test7DayArray = last7DayArr
            # ========= TESTING ONLY ========
            
            if justZeros(last7DayArr) == False:

                self.outputs['days7_all'] = baselineStatistic(last7DayArr)
                
                # Get time slice subset - this time every day for the last 7
                slicedArr = getSliceTimeOfDay(last7DayArr, hoi=self.baselineTimeStamp.hour, pad=3, flat=1)

                # ========= TESTING ONLY ========
                self.testDailyArray = last7DayArr
                # ========= TESTING ONLY ========
                
                self.outputs['days7_daily']  = baselineStatistic(slicedArr)
                
                # PREVIOUS 30 HOURS
                last30HrsArr = last7DayArr[-1800:]

                # ========= TESTING ONLY ========
                self.test30hrArray = last30HrsArr
                # ========= TESTING ONLY ========
                
                if justZeros(last30HrsArr) == False:

                    self.outputs['hrs30_all']  = baselineStatistic(last30HrsArr)
            
        # Do the final insert of the baseline document
        doc = self.buildDoc()
        
        upsertBaselineDoc(self.dbh, doc, self.outputs)

#=======================================================================================================
def main():

    
    # Build the baseline objects as we go so that they can be updated at the end of the period.
    base = bl.baseline(kywd=kwObj, c=c, dbh=dbh, baselinePrecision=baselinePrecision)
    
    # Does the baseline document need updating?
    #if base.needUpdate == True:
        
        # This method takes care of update and insert
    #    base.processBaseline(blankData)
    
    
    
#---------------------------------------------------------------------------------------------------------#
"""                      last baseline
                       /
                     /        tweets
baseline period    /        / |
<-----------------|       /   |
                  |------*----*----| 
                cell build period   \
                                     \
                                      \
                                       Current baseline run time
                                       
Where the baseline is actually calculated for every keyword that comes
in so that the values stored in the last baseline are as current as they
need to be for anomaly detection.


"""