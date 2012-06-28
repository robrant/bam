import sys

importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import ConfigParser
import os

import mdb
from pymongo import GEO2D, ASCENDING, DESCENDING

#------------------------------------------------------------------------------------------ 

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
        self.verbose  = self.config.getboolean("default", "verbose")
        
        # Mongo parameters
        self.host     = self.config.get("mongo", "host")
        self.port     = self.config.getint("mongo", "port")
        self.db       = self.config.get("mongo", "db")
        try:
            self.user     = self.config.get("mongo", "user")
            self.password = self.config.get("mongo", "password")
        except:
            pass
        
        # The collections to be used
        self.alerts         = self.config.get("collections", "alerts")
        self.baseline       = self.config.get("collections", "baseline")
        self.timeseries     = self.config.get("collections", "timeseries")
        self.mapping        = self.config.get("collections", "mapping")
        self.collections    = [self.alerts, self.baseline, self.timeseries, self.mapping]
        
        # Whether to drop the collections and indexes or not before creation
        self.dropDb  = self.config.getboolean("mongo", "dropDb")
        self.dropIdx = self.config.getboolean("mongo", "dropIndexes")

#------------------------------------------------------------------------
            
def setupBaselineIndexes(collectionHandle, dropIndexes=True):
    ''' Sets up the indexes on the baseline collection '''
        
    # SUMMARY: Creates a unique index on the summary collection 
    if dropIndexes==True:
        collectionHandle.drop_indexes()
        
    collectionHandle.create_index([("mgrs",         DESCENDING),
                                   ("keyword",       ASCENDING)],
                                  unique  = True)

#------------------------------------------------------------------------
            
def setupAlertsIndexes(collectionHandle, dropIndexes=True):
    ''' Sets up the indexes on the alerts collection '''

    # ALERTS: Creates a spatial index on the geos
    if dropIndexes==True:
        collectionHandle.drop_indexes()
    
    collectionHandle.create_index([("geo",    GEO2D),
                                  ("keyword",ASCENDING),
                                  ("start",  DESCENDING)],
                                  name = 'alerts_geo_kw_start_idx')

#------------------------------------------------------------------------
            
def setupTimeseriesIndexes(collectionHandle, dropIndexes=True):
    ''' Sets up the indexes on the timeseries collections '''

    # TIMESERIES: Create index on start and keyword
    if dropIndexes==True:
        collectionHandle.drop_indexes()

    # Create a sparse index on 'blank' field.
    collectionHandle.create_index([('blank', ASCENDING)], sparse=True)

    collectionHandle.create_index([("mgrs",           ASCENDING),
                                 ("keyword",        ASCENDING)])
        
    collectionHandle.create_index([("start",          DESCENDING),
                                 ("keyword",        ASCENDING)])
    
    collectionHandle.create_index([("start",          DESCENDING),
                                 ("keyword",        ASCENDING),
                                 ("mgrs",           ASCENDING)])
    
    collectionHandle.create_index([("start",          DESCENDING),
                                 ("keyword",        ASCENDING),
                                 ("mgrs",           ASCENDING),
                                 ("mgrsPrecision", ASCENDING)])

#--------------------------------------------------------------------------------------

def main():
    ''' Builds the collections and indexes needed for the bam mongo work.
        # See also /src/tests/testMdb for full tests of the base functions. '''
    
    path = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/config"
    #path = 'home/dotcloud/code/config/'
    file = "mongoSetup.cfg"
    params = getConfig(path,file)
    
    # Get a db handle
    if params.verbose==True:
        print "Get Mongo Handle."
    c, dbh = mdb.getHandle(host=params.host, port=params.port, db=params.db)

    # Set up collections
    if params.verbose==True:
        print "Setup the mongo collections."
    
    mdb.setupCollections(c, dbh, params.db, params.collections, params.dropDb)

    # Get the collection handles
    timeSeriesHandle = dbh[params.timeseries]
    baselineHandle   = dbh[params.baseline]
    alertsHandle     = dbh[params.alerts]
    mappingHandle    = dbh[params.mapping]
    
    # Set up the indexes on the collections
    if params.verbose==True:
        print "Setup the mongo indexes."
    
    setupTimeseriesIndexes(timeSeriesHandle, dropIndexes=params.dropIdx)
    setupAlertsIndexes(alertsHandle, dropIndexes=params.dropIdx)
    setupBaselineIndexes(baselineHandle, dropIndexes=params.dropIdx)
        
    # Close the connection
    if params.verbose==True:
        print "Closing the connection."
    
    mdb.close(c, dbh)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConnection']
    main()