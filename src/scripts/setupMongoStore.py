import sys

importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import mdb

def main():
    ''' Builds the collections and indexes needed for the bam mongo work.
        # See also /src/tests/testMdb for full tests of the base functions. '''
    
    # Get a db handle
    print "Get Mongo Handle."
    c, dbh = mdb.getHandle()
    
    # Set up collections
    print "Setup the mongo collections."
    dbh = mdb.setupCollections(dbh, dropCollections=1)
    
    # Set up the indexes on the collections
    print "Setup the mongo indexes."
    dbh = mdb.setupIndexes(dbh, dropIndexes=1)
    
    # Close the connection
    print "Close the connections."
    mdb.close(c, dbh)
    

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConnection']
    main()