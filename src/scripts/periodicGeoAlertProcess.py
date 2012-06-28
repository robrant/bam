import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

# Standard libs
import datetime
import time

# FOSS
import numpy as np          # Array handling
import geojson              # Geojson lib

# Custom Libs
import Geographic           # For several geographic functions
import mdb                  # Wrapper for Mongo Connections
import mgrs as mgrsLib      # A C library for mgrs <--> LL conversion
import jmsCode              # Access to JMS communicating functions

def getActiveCells(collectionHandle, timeStamp, lookback=60, mgrsPrecision=None):
    ''' Retrieves those cells that were active in the last minute - determined thru
        the baseline collection. Lookback in seconds. TESTED.'''

    # Output list of [mgrs, keyword, mgrsPrecision] lists
    results = []
    
    # Get the time window in minutes
    minTimeStamp = timeStamp - datetime.timedelta(seconds=lookback)
    
    # Retrieve docs in the last <lookBack> period
    query = {'timeStamp':{'$gte': minTimeStamp,
                          '$lt' : timeStamp}}
    if mgrsPrecision:
        query['mgrsPrecision']=int(mgrsPrecision)
    
    # Using a projection to ensure it only returns these fields
    #projection = {'_id':0, 'keyword':1, 'mgrs':1}
    
    cursor = collectionHandle.find(query)
    results = [res for res in cursor]

    time.sleep(5)

    return results

#-------------------------------------------------------------------------------------------

def getCountsForActiveCells(collectionHandle, timeStamp, lookback, mgrs, mgrsPrecision, keyword):
    ''' Gets the total count for this mgrs/keyword over the period of interest (since last baseline).
        Will only work when period of interest doesn't span multiple days because it is built to
        retrieve and get counts based on indexes.
        
        Lookback in seconds.
        
        '''
    
    minTimeStamp = timeStamp - datetime.timedelta(seconds=lookback)
    
    # Query start time
    dayStart = minTimeStamp.replace(hour=0,minute=0,second=0,microsecond=0)
    
    # Build the query
    q = {'mgrs'           : str(mgrs),
         'mgrs_precision' : mgrsPrecision,
         'keyword'        : str(keyword),
         'start'          : dayStart
         }
    
    try:   
        doc = list(collectionHandle.find(q))[0]
    except Exception, e:
        print e
        print "Failed to retrieve results from timeseries with the following query:\n %s " %(q)
        return 0
    
    # Get the hour and minute indexes for this period
    startIdx = minTimeStamp.hour*60 + minTimeStamp.minute
    
    # Account for runs on the hour
    if timeStamp.hour == 0 and timeStamp.minute == 0:
        hr = 23
        mn = 60
    else:
        hr = timeStamp.hour
        mn = timeStamp.minute
    
    endIdx   = hr*60 + mn
    
    # Build an array of all the data within the query bounds (multiple days)
    data = np.array(doc['data']).flatten()
    
    # Get the sum of the values in that time series
    docSum = data[startIdx:endIdx].sum()

    return docSum

#-------------------------------------------------------------------------------------------

def buildPolygon(mgrs=None, mgrsPrecision=10):
    ''' Builds a polygon that represents the mgrs box.
        TESTED - NEED TO CHECK. '''

    if not mgrs:
        print 'No MGRS provided. Please provide one.'
        return None
    
    m = mgrsLib.MGRS()
    minLat, minLon = m.toLatLon(str(mgrs))
    
    
    # Get the sidelength of 1 cell
    sideLength = Geographic.getMgrsPrecision(mgrsPrecision)
    if not sideLength:
        print 'couldnt determine mgrs precision: mgrs: %s. mgrsPrecision: %s ' %(mgrs, mgrsPrecision)
        return None

    # Determine the cell side length from MGRS precision
    latScale, lonScale = Geographic.radialToLinearUnits(minLat, units='metres')

    # Get the other vertices in lat lon
    maxLat = minLat + (latScale * sideLength)
    maxLon = minLon + (lonScale * sideLength)
    
    # Build a python list of lat lons and return that list. 
    bl = [minLat, minLon]
    tl = [maxLat, minLon]
    tr = [maxLat, maxLon]
    br = [minLat, maxLon]
    
    coords = [bl, tl, tr, br]
    
    return coords

#----------------------------------------------------------------------------------------------------
    
def getThisMinute(timeStamp):
    ''' Gets the timestamp for this minute.
        THINK ABOUT MAKING THIS A MORE GENERALLY ACCESSIBLE FUNCTION.
        NOT TESTED.'''
    
    timeStamp = timeStamp.replace(second=0, microsecond=0)
    return timeStamp

#-------------------------------------------------------------------------------------------

def buildGeoJson(keyword, coords, mgrs, mgrsPrecision, timeStamp, duration, value, anomalies=None):
    ''' Builds a geoJson document from polygon coords.
        NEED TO CHECK THE FORMAT OF THIS GEOJSON. From Matt's email, think
        about building them together into a group of featuresd.
        NEEDS TESTING.'''

    # Deal with the properties
    props={'mgrsPrecision' : mgrsPrecision,
           'mgrs'          : mgrs,
           'timeStamp'     : timeStamp.strftime('%Y-%m-%dT%H:%M:%S'),
           'duration'      : duration.seconds,
           'value'         : value}
    
    # If its an anomaly geoJson, submit that information too
    if anomalies:
        for key in anomalies.keys():
            props[key] = anomalies[key]
    
    # The feature collection
    geoms = []
    
    # The geometry for this keyword/timestamp/mgrs
    p = geojson.Polygon(coordinates=[coords])
    
    # Build the feature
    geom = geojson.feature.Feature(
                                  id=keyword,
                                  geometry=p,
                                  properties=props
                                  )
    geoms.append(geom)
    collection = geojson.feature.FeatureCollection(features=geoms)

    # Dump the geojson as a string
    geoJsonDoc = geojson.dumps(collection)
    
    # Return the geojson doc as a dictionary
    return geoJsonDoc

#-------------------------------------------------------------------------------------------

def reformatGeoJsonTime(geoJsonString):
    '''Gets the timestamp into a datetime for mongo.'''
    
    # Then dump and load it to get it into a dictionary
    geoJsonDoc    = geojson.loads(geoJsonString)
    geoJsonDoc['features'][0]['properties']['timeStamp'] = datetime.datetime.strptime(geoJsonDoc['features'][0]['properties']['timeStamp'], '%Y-%m-%dT%H:%M:%S')
    return geoJsonDoc

#-------------------------------------------------------------------------------------------

def insertGeoJson(collectionHandle, geoJsonDoc):
    ''' Inserts a geojson document'''

    # Get the document into a single element so that its separate from the OID
    doc = {'geo':geoJsonDoc}
    
    response = collectionHandle.insert(geoJsonDoc)
    
    return response

#-------------------------------------------------------------------------------------------

def checkForAnomalies(doc, currentCount):
    ''' Checks to see whether any of the fields are anomalous'''

    anomaliesFound = {}
    baseVals = {}

    # Get any element that is a baseline metric - this could be improved.
    for key in doc.keys():
        if key.find('hrs') >= 0 or key.find('days') >= 0:
            baseVals[key] = doc[key]
            if float(currentCount) > float(doc[key]):
                anomaliesFound[key] = baseVals[key]
        
    if len(anomaliesFound) == 0:
        anomaliesFound = None
        
    return anomaliesFound

#-------------------------------------------------------------------------------------------


def main(timeStamp=None, mgrsPrecision=0, lookback=600, baseColl='baseline', cellColl='mapping', alertsColl='alerts', tsColl='timeseries'):
    
    '''
    Function runs periodically and checks a series of values against the baseline.
    
    1. Retrieves all the active keyword/mgrs combinations that were active in the last period
    2. Gets the count for each of those cells
    3. GEO: Builds an mgrs cell
    4. GEO: Builds a geojson object
    5. GEO: Inserts that into a mapping collection
    
    6. ANO: 
    
    '''

    # Make the JMS connection via STOMP and the jmsCode class
    destination = '/topic/test.bam.alerts'
    hostIn      = 'localhost'
    portIn      = 61613
    jms = jmsCode.jmsHandler(hostIn, portIn, verbose=True)
    jms.connect()

    # Mongo connection parameters
    host = 'localhost'
    port = 27017
    db   = 'bam'

    # Time Variables
    if not timeStamp:
        now = datetime.datetime.utcnow()
    else:
        now = timeStamp
    
    nowMinute = getThisMinute(now)
    
    # Connect and get handle
    c, dbh = mdb.getHandle(host, port, db)
    
    # Assign collection handles to variables for easier passing
    baseCollHandle  = dbh[baseColl]
    tsCollHandle    = dbh[tsColl]
    mapCollHandle   = dbh[cellColl]
    
    # Retrieve the active cells
    activeCells = getActiveCells(baseCollHandle, timeStamp=now, lookback=lookback, mgrsPrecision=mgrsPrecision)
    
    # Loop those active cells
    for activeCell in activeCells:
    
        kywd = activeCell['keyword']
        mgrs = activeCell['mgrs']
        
        # The period for this count value
        duration = datetime.timedelta(seconds=lookback)
        
        # The coordinates of the polygon to be mapped from MGRS
        coords = buildPolygon(mgrs, mgrsPrecision)
        
        # The total count value for this mgrs/keyword/mgrsPrecision
        count = getCountsForActiveCells(tsCollHandle, nowMinute, lookback, mgrs, mgrsPrecision, kywd)
        
        # ANOMALY: Get a list of metrics that indicated it was anomalous
        anomalies = checkForAnomalies(activeCell, count)
        
        # A geoJson object representing all of this information
        geoJson = buildGeoJson(kywd, coords, mgrs, mgrsPrecision, now, duration, count, anomalies)
        
        # ANOMALY: If it was anomalous, push the geoJson to JMS
        jms.sendData(destination, geoJson)
        
        # Insert the geoJson into the mapping collection
        success = insertGeoJson(mapCollHandle, reformatGeoJsonTime(geoJson))
        
    jms.disConnect()
    mdb.close(c, dbh)
    
#========================================================================================================


startDate = datetime.datetime(2011,5,2,0,0,0)
mgrsPrecision = 10

for i in range(0,100):
    
    processDate = startDate + datetime.timedelta(seconds=i*600)    
    main(timeStamp=processDate, mgrsPrecision=mgrsPrecision, lookback=600, baseColl='baseline', cellColl='mapping', tsColl='timeseries', alertsColl='alerts')



#main(timeStamp=datetime.datetime(2011,5,2,0,0,0), mgrsPrecision=2, lookback=600, baseColl='baseline', cellColl='mapping', tsColl='timeseries', alertsColl='alerts')