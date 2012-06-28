import unittest
import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

import calculateBaseline as cbl
import numpy as np
import random

class Test(unittest.TestCase):


    def testnormalStd95percentZeros(self):
        ''' Validates the normal/std approach'''
        
        # Define houw big the array is and build it
        days = 7
        hours = 24
        minutes = 60
        bins = days * hours * minutes
        print "Number of Bins/minutes: %s" % bins
        
        # Build the array
        arr = np.zeros((bins), dtype=np.int8)
        
        # What percent of the array do you want with values?
        percent = 50.0
        fraction = float(percent)/100.0
        print fraction
        randomSet = []
        
        # Populate <percent> number of elements of the array
        for j in range(int(bins*fraction)):
            idx = random.randint(0, bins)
            randomSet.append(idx)

        print "Number of bins modified: %s" %j

        # Add some values into the array
        for i in range(bins):
            if i in randomSet:
                arr[i] = random.randint(1,2)
        threshold = cbl.normalStd(arr, std=2)
        
        print threshold


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testnormalStd']
    unittest.main()