import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import unittest
from scriptGetVast import *

class TestGetVastFunctions(unittest.TestCase):

    def testRetrieveFile(self):
        ''' Get the file to a handle. '''
        
        fn = "fileForVastTest.txt"
        path = "/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/"
        f = retrieveFile(path, fn)
        self.assertEqual(f.closed, False)
        f.close()
        
    def testGetGeos(self):
        ''' Retrieve of Geos from the tweet.'''
        
        tId = 3569
        geos = [34.5, 45.5]
        geosIn = ' '.join(list(str(g) for g in geos))
        geosOut= getGeos(tId, geosIn)
        self.assertEquals(geos, geosOut)
        
        
    def testGetTime(self):
        ''' Retrieve the time from the tweet.'''
        
        tId = 3569
        truth  = datetime.datetime(2011,1,1,12,1)
        dtgIn  = '1/1/2011 12:01'
        dtgOut = getTime(tId, dtgIn)
        self.assertEquals(dtgOut, truth)

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()