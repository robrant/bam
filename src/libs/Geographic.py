import numpy
from math import *
import re
import math

A  = 6378.137  #WGS earth radius
Re = A # As above but 2 different conventions are used.
E  = 0.0818191908426 # Earth's eccentricity

radian = 180./pi

#----------------------------------------------------------------------#

def utm2Mgrs(zone, easting, northing, d=10):

    '''Converts UTM (universal tranverse mercator) coordinates
    (WGS84 geodetic) to MGRS (military grid reference system)
    Constant from math library: e = 2.7182818284590451.
    Unit-tests have been written for this function.'''


    #d = 10 # number of figures in the grid reference - don't change this

    # Validate that it's a sensible grid precision
    #(must be 1 - 11 figs and an even number. Therefore: 2,4,6,8,10)
    if (d > 0) and (d < 12) and ((d%2) == 0):
        ok = True
    else:
        ok = False

    # Get the zone
    a = zone + ""
    g = []
    g = [zone, easting, northing]

    #Get the numerical grid zone
    z = a[:-1]
    ez = "ABCDEFGHJKLMNPQRSTUVWXYZ"

    f  = ""
    i = 0

    # Loops the easting, then northing
    while i < 2:

        # Get the coordinate (easting, then northing in the while loop)
        b = g[i + 1]

        # Calculate j
        j = b//100000
        b -= j*100000

        # Easting calculation
        if i==0:
            if (j<1) or (j>8):
                ok = False
            j += ((int(z) - 1)%3)*8 -1
            
        # Northing Calculation
        else:
            if (int(z)%2)==0:
                j += 5
            while j > 19:
                j -= 20

        # Determines the grid successfully (based on J)
        if j>=0 and j < (24 - 4*i):
            a += ez[j]
        else:
            ok = False

        # Identify the precision of the coordinate
        # If recoding this, watch the use of math.e = 2.718....
        if b > 0:
            j1alog = float(log(float(b))) * float(log10(math.e))
            j = floor(j1alog)

        else:
            j=0

        string0 = "00000"
        string1 = string0[0:int(4-j)]
        subInt0 = string1 + str(b)
        subStr0 = str(subInt0)
        
        lastchar = d//2
        
        f += "" + str(subStr0[0:lastchar])
        i += 1

    if (ok == True):
        a = a + f

    else:
        a = a + f

    return a


#----------------------------------------------------------------------#

def geo2Utm(lat, lon):
    ''' Converts degrees decimal coordinates (WGS84 geodetic) to UTM
        (Universal Transverse Mercator) coordinates. Use case:
        lat = 45.5, lon = 34.4 :: UTM = 36T 609377 5039450.
        Unit-tests have been written for this function.'''

    smaE = 6378137      # Semi-major axis (meters) of the Earth Ellipsoid WGS84
    e2   = 0.00669438   # Earth's eccentricity squared
    A = 1 - e2/4*(1 + 3*e2/16*(1 + 5*e2/12))
    B = 3*e2/8*(1 + e2/4*(1 + 15*e2/32))
    C = 15*e2*e2*(1 + 3*e2/4)/256
    D = 35*e2*e2*e2/3072

    k = 7       # number of decimal places of Northing and Easting
    zone1 = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    g = []
    g = [lat,lon]

    cm = 3 + 6*(lon//6)
    intcm = int(cm)
    zone = (1 + (cm + 180)//6)
    intzone = int(zone)
    zone = str(intzone)
    j = (lat//8) + 12
    intj = int(j)
    zone += str(zone1[intj])

    lon -= cm
    toRad = pi/180.0
    lat *= toRad
    lon *= toRad
    #print lon
    
    b = 0.9996*smaE
    c = 1 - e2
    e = cos(lat)
    f = tan(lat)
    h = e*e
    M = b*(A*lat - B*2*h*f + C*sin(4*lat) - D*sin(6*lat))
    n = h*e2/c
    t = f*f
    v = b/sqrt(1 - t*h*e2)
    vp = lon*e
    vp2 = vp*vp

    e = (((((2*t - 58)*t + 14)*n + (t -18)*t + 5)*vp2/20 + n -t + 1)*vp2/6 + 1)*v*vp
    
    e += 500000
    k = pow(10,k)
    
    e = round(e*k)/k

    n = ((((t -58)*t + 61)*vp2/30 + (9 + 4*n)*n - t + 5)*vp2/12 + 1)*v*vp2*f/2 + M

    if (lat < 0):
        n += 10000000
    n = round(n*k)/k

    return zone, int(round(e)), int(round(n))

#-------------------------------------------------------------------------------

def getMgrsPrecision(mgrsPrecision):
    ''' Returns the appropriate mgrs cell side length'''
    
    if mgrsPrecision==10:
        sideM = 1.0
    elif mgrsPrecision==8:
        sideM = 10.0
    elif mgrsPrecision==6:
        sideM = 100.0
    elif mgrsPrecision==4:
        sideM = 1000.0
    elif mgrsPrecision==2:
        sideM = 10000.0
    else:
        'print MGRS precision out of range.'
        sideM = None
        
    return sideM

#-------------------------------------------------------------------------------

def radialToLinearUnits(latitude):
    
    ''' Calculates the length of 1 degree of latitude and longitude at
        a given latitude. Takes as arguments the latitude being worked at.
        Returns the length of 1 degree in metres. NEEDS TESTING '''
    
    # Work in radians
    lat = math.radians(latitude)
    
    # Constants
    m1 = 111132.92
    m2 = -559.82
    m3 = 1.175
    m4 = -0.0023
    p1 = 111412.84
    p2 = -93.5
    p3 = 0.118
    
    # Length of a degree in Latitude
    latLen = m1 + (m2 * math.cos(2 * lat)) + (m3 * math.cos(4 * lat)) + \
             (m4 * math.cos(5 * lat))
             
    lonLen = (p1 * math.cos(lat)) + (p2 * math.cos(3*lat)) + (p3 * math.cos(5*lat))
        
    return latLen, lonLen


#-------------------------------------------------------------------------------

class constants(object):
    
    def __init__(self):
        ''' A load of constants for MGRS - Lat/lon conversion'''
        
        self.degToRad = pi / 180.0
        self.radToDeg = 180.0 / pi
        
        self.alphaToNumber = {'A':1, 'B':2, 'C':3, 'D':4, 'E':5, 'F':6, 'G':7, 'H':8, 'J':9, 'K':10, 'L':11, 'M':12, 'N':13, 'P':14, 'Q':15, 'R':16, 'S':17, 'T':18, 'U':19, 'V':20, 'W':21, 'X':22, 'Y':23, 'Z':24}
        self.MGRSEastingByZone = ('x', 'A', 'J',
                             'S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J',
                             'S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J','S','A','J',
                             'S')
        self.MGRSNorthingByZone = ('X','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A',
                              'F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A','F','A',
                              'F','A','F','A','F','A','F','A','F')
        self.MGRSMaxMinNorthingZone = { 'C': 2100000,
                                        'D': 3000000,
                                        'E': 3800000,
                                        'F': 4700000,
                                        'G': 5600000,
                                        'H': 6500000,
                                        'J': 7400000,
                                        'K': 8300000,
                                        'L': 9200000,
                                        'M': 10000000,
                                        'N': 000000,
                                        'P': 800000,
                                        'Q': 1700000,
                                        'R': 2600000,
                                        'S': 3500000,
                                        'T': 4400000,
                                        'U': 5300000,
                                        'V': 6200000,
                                        'W': 7000000,
                                        'X': 7900000
                                      }
        self.EarthSMaj  = 6378137.0           # Semi major acis of the earth ellipsoid in metres
        self.EarthFlat  = 1.0/298.257223563   # Flattening factor of the earth ellipsoid
        self.EarthEcc2  = 0.0066943799901413800
        self.EarthEccB2 = 0.0067394967422764739950480361585505306720733642578125 # Second Eccentricity Squared
        self.EarthScale = 0.9996
        self.piOver2    = pi/2.0
        
        # True Meridianal Constants
        self.TM_ap = 6367449.1458234153687953948974609375
        self.TM_bp = 16038.508662976264531607739627361297607421875
        self.TM_cp = 16.8326132631502645153886987827718257904052734375
        self.TM_dp = 0.0219844041342621941692581089000668725930154323577880859375
        self.TM_ep = 0.000311483710557233385422294669186982218889170326292514801025390625

#================================================================================================================
        
def mgrsToUtm(inMgrsStrn):

    # Instantiate the constants
    c = constants()

    # Regular expression match the MGRS, breaking it into groups    
    regexMgrs = re.compile(r'^([1-9]|[1-5]\d|60)([CDEFGHJKLMNPQRSTUVWX])?([ABCDEFGHJKLMNPQRSTUVWXY]{2})?(\d{5}\s?\d{5}|\d{4}\s?\d{4}|\d{3}\s?\d{3}|\d{2}\s?\d{2}|\d{1}\s?\d{1})$')
    mgrs = re.search(regexMgrs, inMgrsStrn.upper())

    # Zone and quadrant
    zoneNum = int(mgrs.group(1))
    zoneLtr = mgrs.group(2)
    alphaQuadrant = mgrs.group(3)

    # Break up the easting from northing
    en = mgrs.group(4).replace(' ', '')
    pre = len(en) / 2
    easting = float(en[:pre])
    northing = float(en[pre:])
    
    easting += ((1.0 + c.alphaToNumber[alphaQuadrant[:1]]) - c.alphaToNumber[c.MGRSEastingByZone[zoneNum]]) * 100000
    
    if zoneLtr in ('C','D','E','F','G','H','J','K','L','M'):
        hemi = 'S'
        falseNorthing = 10000000
        northing += (80 - (c.alphaToNumber[c.MGRSNorthingByZone[zoneNum]] - c.alphaToNumber[c.alphaQuadrant[-1:]])) * 100000
        while northing > c.MGRSMaxMinNorthingZone[zoneLtr]:
            northing -= 2000000
    
    else:
        hemi = 'N'
        falseNorthing = 0
        northing += (c.alphaToNumber[alphaQuadrant[-1:]] - c.alphaToNumber[c.MGRSNorthingByZone[zoneNum]]) * 100000
        while northing < c.MGRSMaxMinNorthingZone[zoneLtr]:
            northing += 2000000
            
    #****** IS THIS CORRECT? ******
    #****** WHY DOESNT THIS USE THE FALSE NORTHING? ******
    
    
    return zoneNum, hemi, easting, northing



def utmToLonLat(zoneNum, hemisphere, easting, northing):
    
    c = constants()
    
    # Establish the false easting of central longitude
    centreLon = ((zoneNum - 1.0) * 6.0) - 177.
    centreLonR = centreLon * c.degToRad
    
    #****** IS THIS CORRECT? ******
    #****** WHY ISNT IS USED FURTHER BELOW? ******
    
    falseEasting = 500000.
    
    # Establish false northing of the equator
    if hemisphere.upper() == 'N':
        falseNorthing = 0
    else:
        falseNorthing = 10000000
    
    # Origin - assuming origin latitude of 0 degrees
    TMDist = (northing - falseNorthing) / c.EarthScale
    
    # First Estimate
    subRadius = c.EarthSMaj * (1 - c.EarthEcc2)
    fpLat = TMDist / subRadius
    
    for i in range(5):
        t10 = c.TM_ap * fpLat - c.TM_bp * sin(2.0 * fpLat) + c.TM_cp * sin(4.0 * fpLat) - c.TM_dp * sin(6.0 * fpLat) + c.TM_ep * sin(8.0 * fpLat)
        subRadius = c.EarthSMaj * (1.0 - c.EarthEcc2) / (sqrt(1.0 - c.EarthEcc2 * sin(fpLat)**2))**3
        fpLat += (TMDist - t10) / subRadius

    # Radius of curvature in the meridian
    subRadius = c.EarthSMaj * (1.0 - c.EarthEcc2) / (sqrt(1.0 - c.EarthEcc2 * sin(fpLat)**2))**3

    # Radius of curvature in the prime vertical
    vertRadius = (c.EarthSMaj / sqrt(1.0 - c.EarthEcc2 * sin(fpLat)**2))
    
    # Sine/Cosine terms
    sinLat = sin(fpLat)
    cosLat = cos(fpLat)
    
    # Tangent values
    tanLat = sinLat / cosLat
    tanLat2 = tanLat * tanLat
    tanLat4 = tanLat2 * tanLat2
    eta = c.EarthEccB2 * cosLat**2
    eta2 = eta * eta
    eta3 = eta2 * eta
    eta4 = eta3 * eta
    deltaEasting = easting - falseEasting
    
    if (abs(deltaEasting) < 0.0001):
        deltaEasting = 0.0
        
    # Latitude
    t10 = tanLat / (2.0 * subRadius * vertRadius * c.EarthScale**2)
    t11 = tanLat * (5 + 3*tanLat2 + eta - 4*eta**2 - 9*tanLat2*eta) / (24*subRadius*vertRadius**3*c.EarthScale**4)
    t12 = tanLat * (61 + 90*tanLat2 + 46*eta + 45*tanLat4 - 252*tanLat2*eta - 3*eta2 + 100*eta3 - 66*tanLat2*eta2 - 90*tanLat4*eta + 88*eta4 + 225*tanLat4*eta2 + 84*tanLat2*eta3 - 192*tanLat2*eta4) / (720*subRadius*vertRadius**5 * c.EarthScale**6)
    t13 = tanLat * (1385 + 3633 * tanLat2 + 4095*tanLat4 + 1575*tanLat**6)/(40320 * subRadius * vertRadius**7 * c.EarthScale**8)
    latR = fpLat - deltaEasting**2 * t10 + deltaEasting**4 * t11 - deltaEasting**6 * t12 + deltaEasting**8 * t13
    
    # Difference in Longitude
    t14 = 1 / (vertRadius * cosLat * c.EarthScale)
    t15 = (1 + 2 * tanLat2 + eta) / (6 * vertRadius**3 * cosLat * c.EarthScale**3)
    t16 = (5 + 6*eta + 28*tanLat2 - 3*eta2 + 8*tanLat2*eta + 24*tanLat4 - 4*eta3 + 4*tanLat2*eta2 + 24*tanLat2*eta3) / (120*vertRadius**5*cosLat*c.EarthScale**5)
    t17 = (61 + 662*tanLat2 + 1320*tanLat4 + 720*tanLat**6) / (5040*vertRadius**7 * cosLat * c.EarthScale**7)
    
    deltaLon = deltaEasting*t14 - deltaEasting**3 * t15 + deltaEasting**5 * t16 - deltaEasting**7 * t17
    
    # Longitude
    lonR = (centreLon * c.degToRad) + deltaLon
    
    # Limits Tests
    if (latR > c.piOver2):      # Past north or sout poles
        print 'Failed this test'
        latR = c.piOver2
    elif (latR < 0 - c.piOver2):
        latR = 0 - c.piOver2
    
    if (lonR > pi):
        lonR -= 2 * pi
    elif (lonR < -pi):
        lonR += 2 * pi
        
    latDD = latR * c.radToDeg
    lonDD = lonR * c.radToDeg
    
    return lonDD, latDD

#------------------------

def mgrsToLonLat(inMgrs):
    ''' HAS A PROBLEM - NEEDS TO BE CHECKED AND COMPARED AGAINST THE ORIGINAL.'''
    
    zNum, hemi, e, n = mgrsToUtm(inMgrs)
    lonDD, latDD = utmToLonLat(zNum, hemi, int(float(e)), int(float(n)))
    return lonDD, latDD
