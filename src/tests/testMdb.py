import sys

importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 


import unittest
import mdb


class Test(unittest.TestCase):

    
    def testConnectDisconnect(self):
        ''' Tests the safe closure of the connection.'''
        
        # Get the handle
        c, dbh = mdb.getHandle()
        self.assertEqual(dbh.connection.host, 'localhost')
        
        # Close the connection and check its closed based on host
        c = mdb.close(c,dbh)
        self.assertEqual(dbh.connection.host, None)
    

    def testSetupCollections(self):
        ''' Tests the creation of the collections.'''
        
        # Normal setup
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)
        self.assertEquals(dbh.timeseries._Collection__name, 'timeseries')
        
        # Insert a simple document
        dbh.timeseries.insert({'a':1})
        
        # Retrieve the document and test it
        out = dbh.timeseries.find({'a':1})[0]
        self.assertEquals(out['a'], 1)
        
        # Clean up and drop it
        dbh.timeseries.remove()
        
                # Drop all the collections made here.
        dbh.drop_collection('timeseries')
        dbh.drop_collection('summary')
        dbh.drop_collection('thresholds')
        dbh.drop_collection('alerts')
        dbh.drop_collection('keywords')
        # Close out
        mdb.close(c, dbh)
        
    def testSetupIndexes(self):
        ''' Tests the creation of the indexes on the collections.'''
        
        # Normal setup
        c, dbh = mdb.getHandle()
        
        # Setup collections
        dbh = mdb.setupCollections(dbh, dropCollections=True)
        
        # Setup the indexes
        mdb.setupIndexes(dbh, dropIndexes=True)
        
        # TIMESERIES: Check some info about the index to ensure it created
        idx = dbh.timeseries.index_information()
        self.assertEquals(idx['ts_uniq_start_kw_mgrs_idx']['unique'], True)
        dbh.timeseries.remove()
        
        # ALERTS: Check some info about the index to ensure it created
        idx = dbh.alerts.index_information()
        self.assertEquals(idx['alerts_geo_kw_start_idx']['key'], [('geo','2d'),
                                                                  ('keyword',1),
                                                                  ('start',-1)])
        dbh.alerts.remove()
        
        # SUMMARY: Check some info about the index to ensure it created
        idx = dbh.summary.index_information()
        self.assertEquals(idx['summ_uniq_start_kw_idx']['unique'], True)
        dbh.summary.remove()
        
        # THRESHOLDS: Check some info about the index to ensure it created
        idx = dbh.thresholds.index_information()
        self.assertEquals(idx['thresh_uniq_kw_mgrs_idx']['unique'], True)
        dbh.thresholds.remove()
        
        # Drop all the collections made here.
        dbh.drop_collection('timeseries')
        dbh.drop_collection('summary')
        dbh.drop_collection('thresholds')
        dbh.drop_collection('alerts')
        
        # Close out
        mdb.close(c, dbh)

       
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testConnection']
    unittest.main()