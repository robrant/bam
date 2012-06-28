import sys

from pymongo import Connection
from pymongo.errors import ConnectionFailure
from pymongo import GEO2D, ASCENDING, DESCENDING

def getHandle(host='localhost', port=27017, db='bam'):
    ''' Acquires a mongo database handle or handles the exception'''
    
    # Establish the connection
    try:
        c = Connection(host=host, port=port)
    except ConnectionFailure, e:
        sys.stderr.write("Could not connect to Mongodb: %s " % e)
        sys.exit(1)
    
    # Get a database handle
    dbh = c[db]
    
    assert dbh.connection == c

    return c, dbh

#------------------------------------------------------------------------

def close(connection, dbHandle):
    ''' Handles the proper closing of the connection.'''
    
    connection.disconnect()
    
    return connection

#------------------------------------------------------------------------

def setupCollections(c, dbh, dbName, collections=None, dropDb=True):
    
    ''' Sets up the database and its constituent elements. '''

    if not collections:
        print 'Failed to drop or build any collections. You must provide a list.'
        return None

    # Create each of the collection
    if dropDb == True:
        response = c.drop_database(dbName)
        
    for coll in collections:
        # Create the collections
        try:
            dbh.create_collection(coll)
        except:
            print "Failed to create the collection '%s'." %(coll)
            
