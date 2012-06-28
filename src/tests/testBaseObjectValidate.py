'''
Created on Feb 20, 2012

@author: brantinghamr
'''
import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 
import unittest
import datetime

from baseObjects import validation

class TestValidate(unittest.TestCase):

    def setUp(self):
        '''    '''
        
        self.validate = validation()

    def testValidateTime(self):
        ''' Checks validateTime against a good time. Good times.'''
        
        ts = datetime.datetime(year=2011,month=1,day=1,hour=12,minute=0,second=1)
        tsOut =  self.validate.validateTime(ts)
        self.assertEqual(tsOut, ts)

    def testValidateTimeWrong(self):
        ''' Checks validateTime against a bad time. '''
        
        ts = '2011-01-01T12:00:01'
        tsOut = self.validate.validateTime(ts)
        self.assertEqual(tsOut, None)

    def testValidateGeosGood(self):
        ''' Checks validateGeos against a good set of geos.'''

        lat = 34.4
        lon = 45.5
        
        latOut, lonOut = self.validate.validateGeos(lat, lon)
        self.assertEqual(lat, latOut)
        self.assertEqual(lon, lonOut)

    def testValidateGeosBad(self):
        ''' Checks validateGeos against a bad set of geos.'''

        lat = 98.0
        lon = -180.9
        
        latOut, lonOut = self.validate.validateGeos(lat, lon)
        self.assertEqual(latOut, None)
        self.assertEqual(lonOut, None)
