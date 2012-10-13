import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

# Standard libs
import datetime
import time
import os
import ConfigParser

# FOSS
import numpy as np          # Array handling
import geojson              # Geojson lib

# Custom Libs
import Geographic           # For several geographic functions
import mdb                  # Wrapper for Mongo Connections
import mgrs as mgrsLib      # A C library for mgrs <--> LL conversion
import jmsCode              # Access to JMS communicating functions
import math

#===================================================================================================

class getConfig(object):
    '''    '''

    def __init__(self, path, configFile):
        '''    '''
        
        self.getConfigFile(path, configFile)
        self.getParameters()
        
#--------------------------------------------------------------------------------
    
    def getConfigFile(self, path, file):
        ''' Reads in a list of keywords from a config file. '''
        
        self.config = ConfigParser.ConfigParser()
        try:
            self.config.read(os.path.join(path, file))
        except:
            print "Failed to read the config file for keyword lookup."

#--------------------------------------------------------------------------------            
    
    def getParameters(self):
        
        # Default Parameters
        try:
            self.verbose        = self.config.getboolean("misc", "verbose")
            self.mgrsPrecision  = self.config.getint('misc', "mgrsPrecision")
            self.lookback       = self.config.getint('misc', "lookback")
        except Exception, e:
            print "========== ERROR ON DEFAULT PARAM CONFIG PARSING =========="
            print e
            print "========== ERROR ON DEFAULT PARAM CONFIG PARSING =========="
        
        # Mongo parameters
        try:
            self.mHost = self.config.get("mongo", "host")
            self.mPort = self.config.getint("mongo", "port")
            self.mDb   = self.config.get("mongo", "db")
            try:
                self.user     = self.config.get("mongo", "user")
                self.password = self.config.get("mongo", "password")
            except:
                pass

            self.baseColl   = self.config.get("mongo", "baseCollection")
            self.cellColl   = self.config.get("mongo", "cellCollection")
            self.alertsColl = self.config.get("mongo", "alertsCollection")
            self.tsColl     = self.config.get("mongo", "tsCollection")
            
            self.storeCell  = self.config.getboolean("mongo", 'storeCell')
            
        except Exception, e:
            print "========== ERROR ON MONGO CONFIG PARSING =========="
            print e
            print "========== ERROR ON MONGO CONFIG PARSING =========="

        # JMS Parameters
        try:
            self.publishJms   = self.config.getboolean("jms", "jmsPublish")
            self.jDestination = self.config.get("jms", "jmsDestination")
            self.jHost        = self.config.get("jms", "jmsHost")
            self.jPort        = self.config.getint("jms", "jmsPort")
        except Exception, e:
            print "========== ERROR ON MONGO CONFIG PARSING =========="
            print e
            print "========== ERROR ON MONGO CONFIG PARSING =========="
        
        print self.publishJms
        
#===================================================================================================






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
    q = {'mgrs'          : str(mgrs),
         'mgrsPrecision' : mgrsPrecision,
         'keyword'       : str(keyword),
         'start'         : dayStart
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

def buildPolygon(m, mgrs=None, mgrsPrecision=10):
    ''' Builds a polygon that represents the mgrs box.
        NOT TESTED
         '''

    # Get the SW corner Lat and Lon
    swLat, swLon = m.toLatLon(str(mgrs))
    # Extending a vector into the other cells to retrieve their coords and then mgrs
    offSetMultiplier = 1.5
    
    # Get the sidelength of 1 cell and extends it to ensure the checking vector hits inside the adjacent cell
    displacement = (Geographic.getMgrsPrecision(mgrsPrecision))
    displacement *= offSetMultiplier
    
    # Determine the cell side length from MGRS precision
    latScale, lonScale = Geographic.radialToLinearUnits(swLat)
    scale = 1.0/latScale
    displacement = float(displacement) * float(scale)
    
    nLat, nLon   = getAdjacentCellCoords(m, mgrsPrecision, swLat, swLon, 10.0, displacement)
    neLat, neLon = getAdjacentCellCoords(m, mgrsPrecision, swLat, swLon, 45.0, displacement)
    eLat, eLon   = getAdjacentCellCoords(m, mgrsPrecision, swLat, swLon, 80.0, displacement)
    
    coords = [[swLon, swLat],
              [nLon,  nLat],
              [neLon, neLat],
              [eLon,  eLat],
              [swLon, swLat]]
    
    return coords

#----------------------------------------------------------------------------------------------------
def getAdjacentCellCoords(m, mgrsPrecision, minLat, minLon, inBearing, displacement):
    ''' Displaces a coordinate from a start coordinate (in a plane) to get a new lat/lon.
        Converts that lat/lon back into mgrs for the cell and converts that to Lat/Lon
        to get the SW corner lat and lon.
        
        NOT TESTED
        '''
    
    #___o____
    #|     / 
    #|    /
    #|a  /h
    #|  /
    #| /
    #|/
    
    #Check the Northern adjacent cell
    bearing = math.radians(inBearing)
    lonOffset = math.sin(bearing) * float(displacement)
    latOffset = math.cos(bearing) * float(displacement)
    
    newLat = minLat + latOffset
    newLon = minLon + lonOffset
    print newLat, newLon, displacement
    # Convert back to mgrs
    cellMgrs = m.toMGRS(newLat, newLon, MGRSPrecision=mgrsPrecision/2)
    cellLat, cellLon = m.toLatLon(str(cellMgrs))
    
    return cellLat, cellLon

#----------------------------------------------------------------------------------------------------


"""
def oldBuildPolygon(mgrs=None, mgrsPrecision=10):
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
"""
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
    geoJsonDoc = geojson.dumps(geom)
    
    # Return the geojson doc as a dictionary
    return geoJsonDoc

#-------------------------------------------------------------------------------------------

def reformatGeoJsonTime(geoJsonString):
    '''Gets the timestamp into a datetime for mongo.'''
    
    # Then dump and load it to get it into a dictionary
    geoJsonDoc    = geojson.loads(geoJsonString)
    geoJsonDoc['properties']['timeStamp'] = datetime.datetime.strptime(geoJsonDoc['properties']['timeStamp'], '%Y-%m-%dT%H:%M:%S')
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

def main(timeStamp=None):
    
    '''

    '''
    print 'in main'
    
    # Get the config params into a object
    path = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/config"
    file = "periodicGeoAlert.cfg"
    params = getConfig(path,file)

    # Make the JMS connection via STOMP and the jmsCode class
    if params.publishJms:
        jms = jmsCode.jmsHandler(params.jHost, params.jPort, verbose=params.verbose)
        jms.connect()
        
    # Instantiate the mgrs lib
    m = mgrsLib.MGRS()

    # Time Variables
    if not timeStamp:
        now = datetime.datetime.utcnow()
    else:
        now = timeStamp
    
    nowMinute = getThisMinute(now)
    
    # Connect and get handle
    c, dbh = mdb.getHandle(params.mHost, params.mPort, params.mDb)
    
    # Assign collection handles to variables for easier passing
    baseCollHandle  = dbh[params.baseColl]
    tsCollHandle    = dbh[params.tsColl]
    mapCollHandle   = dbh[params.cellColl]
    
    # Retrieve the active cells
    activeCells = getActiveCells(baseCollHandle, timeStamp=now, lookback=params.lookback, mgrsPrecision=params.mgrsPrecision)
    
    fxx = open(path+'outGeoJson.gjsn', 'w')
    
    # Loop those active cells
    for activeCell in activeCells:
    
        kywd = activeCell['keyword']
        mgrs = activeCell['mgrs']
        print mgrs
        # The period for this count value
        duration = datetime.timedelta(seconds=params.lookback)
        print 'duration', duration
        # The coordinates of the polygon to be mapped from MGRS
        coords = buildPolygon(m, mgrs, params.mgrsPrecision)
        print 'coords: ', coords
        # The total count value for this mgrs/keyword/mgrsPrecision
        count = getCountsForActiveCells(tsCollHandle, nowMinute, params.lookback, mgrs, params.mgrsPrecision, kywd)
        print 'count: %s' %count
        # ANOMALY: Get a list of metrics that indicated it was anomalous
        #anomalies = checkForAnomalies(activeCell, count)
        anomalies = None
        
        # A geoJson object representing all of this information
        geoJson = buildGeoJson(kywd, coords, mgrs, params.mgrsPrecision, now, duration, count, anomalies)
        
        # ANOMALY: If it was anomalous, push the geoJson to JMS
        if params.publishJms == True:
            jms.sendData(params.jDestination, geoJson)
            fxx.write(geoJson+'\n')
            
        # Insert the geoJson into the mapping collection
        if params.storeCell == True:
            success = insertGeoJson(mapCollHandle, reformatGeoJsonTime(geoJson))
            print 'success: %s' %success
            
    #jms.disConnect()
    mdb.close(c, dbh)
    fxx.close()
    
#========================================================================================================

if __name__ == "__main__":

    startDate = datetime.datetime(2012,10,9,13,10,0)
    
    for i in range(0,50):
        
        processDate = startDate + datetime.timedelta(seconds=i*600)    
        print i, processDate
        
        main(timeStamp=processDate)
        #main(timeStamp=processDate, mgrsPrecision=4, lookback=600, baseColl='baseline', cellColl='mapping', tsColl='timeseries', alertsColl='alerts')
        
        