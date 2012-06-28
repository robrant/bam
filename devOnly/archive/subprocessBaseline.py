import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx)

# Standard libs
import sys
from optparse import OptionParser
import datetime

#Custom libs
import mdb
from baselineProcessing import baseline
import timeSeriesDoc as tsd

def main():

    # Should really move these to being 
    parser = OptionParser()
    parser.add_option("-H", "--host",   dest="host")
    parser.add_option("-p", "--port",   dest="port")
    parser.add_option("-d", "--db",     dest="db")
    
    
    parser.add_option("-m", "--mgrs",               dest="mgrs")
    parser.add_option("-M", "--mgrsprecision",      dest="mgrsPrecision")
    parser.add_option("-t", "--timestamp",          dest="timeStamp")
    parser.add_option("-k", "--keyword",            dest="keyword")
    parser.add_option("-u", "--baselineUnit",       dest="baselineUnit")
    parser.add_option("-v", "--baselineValue",      dest="baselineValue")
    
    (options, args) = parser.parse_args()
    
    # Format the option inputs = these really should be arguments
    port              = int(options.port)
    timeStamp         = datetime.datetime.strptime(options.timeStamp, "%Y-%m-%dT%H:%M:%S")
    mgrsPrecision     = int(options.mgrsPrecision)
    baselinePrecision = [options.baselineUnit, int(options.baselineValue)]
    
    c, dbh = mdb.getHandle(host=options.host, port=port, db=options.db)
    
    # Build the baseline objects as we go so that they can be updated at the end of the period.
    base = baseline(options.mgrs, mgrsPrecision, options.keyword, timeStamp, c=c, dbh=dbh, baselinePrecision=baselinePrecision)

    # Does the baseline document need updating?
    if base.needUpdate == True:
        
        # This method takes care of update and insert
        base.processBaseline(tsd.buildBlankData())
        
    try:
        mdb.close(c, dbh)
    except:
        pass
    
#---------------------------------------------------------------------------------------------------------#
"""                      last baseline
                       /
                     /        tweets
baseline period    /        / |
<-----------------|       /   |
                  |------*----*----| 
                cell build period   \
                                     \
                                      \
                                       Current baseline run time
                                       
Where the baseline is actually calculated for every keyword that comes
in so that the values stored in the last baseline are as current as they
need to be for anomaly detection.


"""

if __name__ == "__main__":
    main()