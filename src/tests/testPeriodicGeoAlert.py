import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import unittest
import periodicGeoAlertProcess as pgap
from baseObjects import keyword as kw
from timeSeriesDoc import timeSeries
import mdb
import datetime

class Test(unittest.TestCase):
    
    """
    
    def testgetActiveCells(self):
        '''Get the active cells.'''
        
        # Connect and get handle
        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)

        # Chuck in a dummy baseline doc
        docs = [{"mgrs" : "38SND4595706622", "keyword" : "keyword1", "timeStamp" : datetime.datetime(2012,4,30,14,5), "mgrs_precision" : 10},
                {"mgrs" : "38SND4595706622", "keyword" : "keyword2", "timeStamp" : datetime.datetime(2012,4,30,14,3), "mgrs_precision" : 10},
                {"mgrs" : "38SND4593006630", "keyword" : "keyword2", "timeStamp" : datetime.datetime(2012,4,30,14,5), "mgrs_precision" : 10},
                {"mgrs" : "38SND4506", "keyword" : "keyword2", "timeStamp" : datetime.datetime(2012,4,30,14,1), "mgrs_precision" : 4}]
        
        collectionHandle = dbh['baseline']
        
        for doc in docs:
            collectionHandle.insert(doc)
        
        now = datetime.datetime(2012,4,30,14,10)
        results = pgap.getActiveCells(collectionHandle,
                                      timeStamp=now, 
                                      lookback=600, 
                                      mgrsPrecision=10)
        
        self.assertEquals(len(results), 3)
        
        results = pgap.getActiveCells(collectionHandle,
                                      timeStamp=now, 
                                      lookback=300,
                                      mgrsPrecision=10)
        
        self.assertEquals(len(results), 2)

        results = pgap.getActiveCells(collectionHandle,
                                      timeStamp=now, 
                                      lookback=900, 
                                      mgrsPrecision=4)

        self.assertEquals(len(results), 1)

        # Close the connection
        mdb.close(c, dbh)
        


    def testgetCountsForActiveCells(self):
        '''Get the count for the active cells.'''

        c, dbh = mdb.getHandle()
        dbh = mdb.setupCollections(dbh, dropCollections=True)
        tsCollectionName = 'timeseries'
        tsCollHandle = dbh[tsCollectionName]

        # Populate the timeseries
        testKywd  = kw(keyword='keyword1',
                       timeStamp=datetime.datetime.utcnow(),
                       lat=34.4, lon=45.5,
                       text='this text contained the hashtag #keyword1',
                       tweetID=346664, userID=4444, source='twitter')
        
        # Insert the current keyword - NOTE HOW THIS IS AFTER THE BASELINE BUILD
        ts5 = timeSeries()
        ts5.importData(testKywd)
        success = ts5.insertBlankDoc()
        ts5.updateCount(incrementBy=5000)

        # Process Called
        runTimeStamp = testKywd.timeStamp.replace(second=0) + datetime.timedelta(seconds=60)
        lookback = 600
        
        count = pgap.getCountsForActiveCells(tsCollHandle, runTimeStamp, lookback, testKywd.mgrs, testKywd.mgrsPrecision, testKywd.keyword)
        
        self.assertEquals(count, 5000)
        

        # Close the connection
        mdb.close(c, dbh)

    """
    def testBuildGeoJson(self):
        ''' Build the polygon coordinates based on an mgrs
            Why don't they all fall within each other?
        '''
        
        import geojson
        
        keyword = 'keyword1'
        coords = [[33.329996361253379, 44.399992763098133], [33.330005377546669, 44.399992763098133], [33.330005377546669, 44.400003503805152], [33.329996361253379, 44.400003503805152]]
        mgrs = '38SMB4415988032'
        mgrsPrecision = 10
        timeStamp = datetime.datetime.utcnow()
        duration = datetime.timedelta(seconds=60)
        value = 1000
        anomalies=None
        
        gj = pgap.buildGeoJson(keyword, coords, mgrs, mgrsPrecision, timeStamp, duration, value, anomalies=None)
        
        print gj
        
    """
    def testBuildPolygons(self):
        ''' Build the polygon coordinates based on an mgrs
            Why don't they all fall within each other?
        '''

        kml = '''<?xml version="1.0" encoding="UTF-8"?>
        <kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
        <Document>
            <name>KmlFile</name>
                <Style id="2">
                    <LineStyle>
                        <width>4</width>
                    </LineStyle>
                    <PolyStyle>
                        <color>4d0000ff</color>
                    </PolyStyle>
                </Style>
                <Style id="4">
                    <LineStyle>
                        <width>4</width>
                    </LineStyle>
                    <PolyStyle>
                        <color>4dff0000</color>
                    </PolyStyle>
                </Style>
                <Style id="6">
                    <LineStyle>
                        <width>4</width>
                    </LineStyle>
                    <PolyStyle>
                        <color>4dffff00</color>
                    </PolyStyle>
                </Style>
                <Style id="8">
                    <LineStyle>
                        <width>4</width>
                    </LineStyle>
                    <PolyStyle>
                        <color>4d00ffff</color>
                    </PolyStyle>
                </Style>
                <Style id="10">
                    <LineStyle>
                        <width>4</width>
                    </LineStyle>
                    <PolyStyle>
                        <color>4dffffff</color>
                    </PolyStyle>
                </Style>'''

        mgrsTests = [['38SMB4415988032', 10],
                     ['38SMB44158803', 8],
                     ['38SMB441880', 6],
                     ['38SMB4488', 4],
                     ['38SMB48', 2]]
                     
        for mgrsTest in mgrsTests:  
            
            polygon = pgap.buildPolygon(mgrsTest[0], mgrsTest[1])
            coords = ''
            for pair in polygon:
                coords += "%s,%s,0 " %(pair[1], pair[0])
            
            kml += '''<Placemark> <name>Grid: %s</name> <styleUrl>%s</styleUrl>
                    <gx:balloonVisibility>1</gx:balloonVisibility>
                    <Polygon>
                        <tessellate>1</tessellate>
                        <outerBoundaryIs>
                            <LinearRing>
                                <coordinates>
                                    %s
                                </coordinates>
                            </LinearRing>
                        </outerBoundaryIs>
                    </Polygon>
                </Placemark>
            ''' %(str(mgrsTest[0]), str(mgrsTest[1]), coords)

        kml += "</Document> </kml>"

        f = open('../../docs/mgrsBoxes.kml', 'w')
        f.write(kml)
        f.close()
    """
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testgetActiveCells']
    unittest.main()