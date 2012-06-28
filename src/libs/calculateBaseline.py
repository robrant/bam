import sys
importDir = ['/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/tests/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/scripts/',
             '/Users/brantinghamr/Documents/Code/eclipseWorkspace/bam/src/libs/']
for dirx in importDir:
    if dirx not in sys.path: sys.path.append(dirx) 

# Standard libs
import datetime

# Non-standard FOSS libraries
import numpy as np

def normalStd(arr, std=2):
    ''' Returns the mean and stdev value for the array'''
    
    # Get the mean, std and return
    mean = np.mean(arr)
    stdev = np.std(arr)
    threshold = mean + stdev*std
    
    return threshold

#------------------------------------------------------------------------------

def medianAbsoluteDeviation(arr):
    ''' Returns median absolute deviation.'''
    
    # Get the median for the array
    med = np.median(arr)
    
    # Create an array to hold the ABSOLUTE of (x-med(x)) residuals
    residualsArr = np.zeros((arr.shape))
    
    # Calculate the residuals and put them in the array
    for x in range(len(arr)):
        residualsArr[x] = abs(arr[x] - med)
        
    mad = np.median(residualsArr) * 1.4826
        
    return mad

#------------------------------------------------------------------------------

# Valid statistical plotting for the baseline period? Gamma? Normal? What model? 