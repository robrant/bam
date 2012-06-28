''' 
Unittests for the Geographic module, which contains
functions for converting between different coordinate formats

Tests by: Rich Brantingham. February 2012.

''' 
import Geographic


import unittest


class Test(unittest.TestCase):
    """
    def testGeo2Utm(self):
        ''' Checks the conversion of Geo Lat/lon to UTM coordinates'''

        # Starting coordinates
        lat = 45.5
        lon = 34.4        
        
        #retrieve UTM
        utm = Geographic.geo2Utm(lat, lon)
        #print utm
        
        utmTrue = ('36T', 609377, 5039450)
        self.assertEquals(utmTrue, utm)

    """
    def testGeo2Mgrs(self):
        '''    '''
        
        lat = 45.5
        lon = 34.4        
        
        #retrieve UTM
        zone, easting, northing = Geographic.geo2Utm(lat, lon)
        mgrs = Geographic.utm2Mgrs(zone, easting, northing, d=10)
        print "10 fig", mgrs
        mgrs = Geographic.utm2Mgrs(zone, easting, northing, d=8)
        print "8 fig", mgrs
        mgrs = Geographic.utm2Mgrs(zone, easting, northing, d=6)
        print "6 fig", mgrs
        mgrs = Geographic.utm2Mgrs(zone, easting, northing, d=4)
        print "4 fig", mgrs
        mgrs = Geographic.utm2Mgrs(zone, easting, northing, d=2)
        print "2 fig", mgrs

    """
    #----------------------------------------------------------------------------------

    def testUtm2Mgrs(self):
        ''' Checks the conversion from UTM to MGRS '''
        
        utm = ['36T', 609377, 5039450]
        
        mgrs = Geographic.utm2Mgrs(utm[0], utm[1], utm[2], 10)
        truthMgrs = "36TXR0937739450"
        self.assertEquals(mgrs, truthMgrs)

    #----------------------------------------------------------------------------------

    def testUtm2Mgrs_4FGR(self):
        ''' Checks the conversion from UTM to MGRS - lower precision: 4 figure grid ref. '''
        
        utm = ['36T', 609377, 5039450]
        
        mgrs = Geographic.utm2Mgrs(utm[0], utm[1], utm[2], 4)
        truthMgrs = "36TXR0939"
        self.assertEquals(mgrs, truthMgrs)
    """
    #----------------------------------------------------------------------------------
    
    def testMgrsToLonLat(self):
        ''' Checks the conversion from MGRS to UTM. '''

        f10 = "36TXR0937739450"
        f8 = "36TXR09373945"
        f6 = "36TXR093394"
        f4 = "36TXR0939"
        f2 = "36TXR03"
        
        fs = [f10, f8, f6, f4, f2]

        for mgrs in fs:
            lat, lon = Geographic.mgrsToLonLat(mgrs)
            print lat, lon
            #self.assertAlmostEquals(lon, 45.5, 3)
            #self.assertAlmostEquals(lat, 34.4, 3)

    #----------------------------------------------------------------------------------

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testUtm2Mgrs']
    unittest.main()

