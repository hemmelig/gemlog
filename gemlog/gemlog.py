## gemlog changes:
## new gps column (time)
## metadata time format change

## known potential problems:
## missing/repeated samples at block boundaries due to InterpGem at the beginning
## bitweight missing in output mseed
## not starting files on the hour
## doesn't handle nearly-empty raw files well

## fixed issues:
## from gemlog ReadGemv0.85C (and others too): NaNs in L$gps due to unnecessary and harmful doubling of wna indexing. Also, added python code to drop NaNs.
import pdb
#pdb.set_trace()
#import rpy2 ## needed for R types. if omitted, can cause "malformed file" error.
#import rpy2.robjects.packages as rpackages
import warnings
import numpy as np
from numpy import NaN, Inf
import os, glob, csv, time, scipy
import pandas as pd
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import obspy

#import obspy

#import obspy.io.mseed.core
#import matplotlib.pyplot as plt
#gemlogR = rpackages.importr('gemlog')
#####################
def Convert(rawpath = '.', convertedpath = 'converted', metadatapath = 'metadata', metadatafile = '', gpspath = 'gps', gpsfile = '', t1 = -Inf, t2 = Inf, nums = NaN, SN = '', bitweight = NaN, units = 'Pa', time_adjustment = 0, blockdays = 1, fileLength = 3600, station = '', network = '', location = '', fmt = 'MSEED'):
    ## bitweight: leave blank to use default (considering Gem version, config, and units). This is preferred when using a standard Gem (R_g = 470 ohms)
    
    ## make sure the raw directory exists and has real data
    assert os.path.isdir(rawpath), 'Raw directory ' + rawpath + ' does not exist'
    assert len(glob.glob(rawpath + '/FILE' +'[0-9]'*4 + '.???')) > 0, 'No data files found in directory ' + rawpath

    #pdb.set_trace()
    ## make sure bitweight is a scalar
    if((type(nums) == type(1)) or (type(nums) == type(1.0))):
        nums = np.array([nums])
    else:
        nums = np.array(nums)

    ## if 'nums' is default, convert all the files in this directory
    if((len(nums) == 0) or np.isnan(nums[0])):
        if(True or len(SN) == 1): # trying to check that SN is a scalar
            fn = glob.glob(rawpath + '/FILE' +'[0-9]'*4 + '.' + SN) + \
                 glob.glob(rawpath + '/FILE' +'[0-9]'*4 + '.TXT')
        else:
            fn = glob.glob(rawpath + '/FILE' +'[0-9]'*4 + '.???')
        nums = np.array([int(x[-8:-4]) for x in fn]) # "list comprehension"
    nums.sort()

    ## start at the first file in 'nums'
    n1 = np.min(nums)
  
    ## read the first set of up to (24*blockdays) files
    L = NewGemVar()
    while((L['data'].count() == 0) & (n1 <= max(nums))): ## read sets of files until we get one that isn't empty
        nums_block = nums[(nums >= n1) & (nums < (n1 + (12*blockdays)))] # files are 2 hours, so 12 files is 24 hours
        L = ReadGem(nums_block, rawpath, SN = SN, network = network, station = station, location = location)
        n1 = n1 + (12*blockdays) # increment file number counter

    p = L['data']
    
    ## if bitweight isn't set, use the default bitweight for the logger version, config, and units
    if(np.isnan(bitweight)):
        if(units == 'Pa'):
            bitweight = L['header']['bitweight_Pa'][0]
        elif(units == 'V'):
            bitweight = L['header']['bitweight_V'][0]
        elif (units == 'Counts') | (units == 'counts'):
            bitweight = 1
      
    ## if not specified, define t1 as the earliest integer-second time available
    if(np.isinf(float(t1))):
        t1 = L['data'].stats.starttime
        t1 = obspy.core.UTCDateTime(np.ceil(float(t1)))

    if(np.isinf(float(t2))):
        t2 = obspy.core.UTCDateTime.strptime('9999-12-31 23:59:59', '%Y-%m-%d %H:%M:%S') # timekeeping apocalypse
  
    wsn = 0
    while(len(SN) < 3): # take the first non-NA SN. This is important because there can be blank files in there.
        wsn = wsn+1
        SN = L['header']['SN'][wsn]
  
    ## set up the gps and metadata files. create directories if necessary
    if(len(gpsfile) == 0):
        if(not os.path.isdir(gpspath)):
            os.makedirs(gpspath) # makedirs vs mkdir means if gpspath = dir1/dir2, and dir1 doesn't exist, that dir1 will be created and then dir1/dir2 will be created
        gpsfile = makefilename(gpspath, SN, 'gps')

  
    if(len(metadatafile) == 0):
        if(not os.path.isdir(metadatapath)):
            os.makedirs(metadatapath)
        metadatafile = makefilename(metadatapath, SN, 'metadata')
  
    ## if the converted directory does not exist, make it
    if(not os.path.isdir(convertedpath)):
        os.makedirs(convertedpath)
  
    ## start metadata and gps files
    metadata = L['metadata']   
    gps = L['gps']
    metadata.to_csv(metadatafile, index=False) ## change to metadata format. need to make ScanMnetadata compatible with both

    wgps = (gps['t'] > (t1 - 1)) ## see ReadGemPy to get gps.t working. update gemlog accordingly.
    if(len(wgps) > 0):
        gps[wgps].to_csv(gpsfile, index=False)

    writeHour = max(t1, p.stats.starttime)
    writeHour = WriteHourMS(p, writeHour, fileLength, bitweight, convertedpath, fmt=fmt)
    
    ## read sets of (12*blockdays) files until all the files are converted
    while(True):
        ## check to see if we're done
        if(n1 > np.max(nums)):# & len(p) == 0):
            break # out of raw data to convert
        if((t1 > t2) & (not np.isnan(t1 > t2))):
            break # already converted the requested data
        ## load new data if necessary
        tt2 = min(t2, truncUTC(t1, 86400*blockdays) + 86400*blockdays)
        #print([tt2, n1, nums, SN])
        while((p.stats.endtime < tt2) & (n1 <= max(nums))):
            L = ReadGem(nums[(nums >= n1) & (nums < (n1 + (12*blockdays)))], rawpath, SN = SN)
            #pdb.set_trace()
            if(len(L['data']) > 0):
                p = p + L['data']   
            #print(p)
            n1 = n1 + (12*blockdays) # increment file counter
            if(len(L['data']) == 0):
                next # skip ahead if there aren't any readable data files here
                
            ## process newly-read data
            if(any(L['header'].SN != SN) | any(L['header'].SN.apply(len) == 0)):
                #breakpoint()
                w = (L['header'].SN != SN) | (L['header'].SN.apply(len) == 0)
                print('Wrong or missing serial number(s): ' + L['header'].SN[w] + ' : numbers ' + str(nums[np.logical_and(nums >= n1, nums < (n1 + (12*blockdays)))][w]))
            
            ## start metadata and gps files
            metadata = L['metadata']   
            gps = L['gps']
            metadata.to_csv(metadatafile, index=False, mode='a', header=False)
            wgps = (gps['t'] > (t1 - 1)) ## see ReadGemPy to get gps.t working. update gemlog accordingly.
            ## update the gps file
            if(len(wgps) > 0):
                gps.to_csv(gpsfile, index=False, mode='a', header=False)
                
        ## run the conversion and write new converted files
        #if((pp.stats.endtime >= t1) & (pp.stats.starttime <= tt2))):
        while((writeHour + fileLength) <= p.stats.endtime):
            writeHour = WriteHourMS(p, writeHour, fileLength, bitweight, convertedpath, fmt=fmt)
            
        ## update start time to convert
        p.trim(writeHour, t2)
        t1 = truncUTC(tt2+(86400*blockdays) + 1, 86400*blockdays)
    ## while True
    ## done reading new files. write what's left and end.
    while((writeHour <= p.stats.endtime) & (len(p) > 0)):
        writeHour = WriteHourMS(p, writeHour, fileLength, bitweight, convertedpath, fmt=fmt)
        p.trim(writeHour, t2)

def WriteHourMS(p, writeHour, fileLength, bitweight, convertedpath, writeHourEnd = np.nan, fmt='mseed'):
    #pdb.set_trace()
    if(np.isnan(writeHourEnd)):
        writeHourEnd = truncUTC(writeHour, fileLength) + fileLength
    pp = p.copy()
    pp.trim(writeHour, writeHourEnd)
    pp.stats.calib = bitweight
    fn = MakeFilenameMS(pp, fmt)
    pp = pp.split() ## in case of data gaps ("masked arrays", which fail to write)
    if(len(pp) > 0):
        print(pp)
        if(fmt.lower() == 'wav'):
            for i in range(len(pp)):
                ## this is supposed to work for uint8, int16, and int32, but actually only works for uint8. obspy bug?
                pp[i].data = np.array(pp[i].data, dtype = 'uint8')# - np.min(pp[i].data)
            pp.write(convertedpath +'/'+ fn, format = 'WAV', framerate=7000, width=1) 
        else:
            pp.write(convertedpath +'/'+ fn, format = fmt, encoding=10) # encoding 10 is Steim 1
        #mseed_core._write_mseed(pp, convertedpath +'/'+ fn, format = 'MSEED', encoding=10)

    writeHour = writeHourEnd
    return writeHour
    
## DONE
####################################
## test command
#rawpath = '/home/jake/Work/Gem_Tests/2019-05-29_RoofTestIsolation/raw/'
#SN = '051'
#Convert(rawpath = rawpath, SN = SN, nums = range(14, 15)) 
#Convert(rawpath = rawpath, SN = SN, nums = range(6,8))
#4,15; 5,15; 6,8; 8,10; :ValueError: cannot convert float NaN to integer
#10,12: no error

####################################

def truncUTC(x, n=86400):
    return obspy.core.UTCDateTime(int(float(x)/n)*n)#, origin='1970-01-01')

def makefilename(dir, SN, dirtype):
    n = 0
    fn = dir + '/' + SN + dirtype + '_'+ f'{n:03}' + '.txt'
    while(os.path.exists(fn)):
        n = n + 1
        fn = dir + '/' + SN + dirtype + '_' + f'{n:03}' + '.txt'

    return fn


def MakeFilenameMS(pp, fmt):
    t0 = pp.stats.starttime
    return f'{t0.year:04}' + '-' +f'{t0.month:02}' + '-' +f'{t0.day:02}' + 'T' + f'{t0.hour:02}' + ':' + f'{t0.minute:02}' + ':' + f'{t0.second:02}' + '.' + pp.id + '.' + fmt.lower()
#import pdb

def ReadGemPy(nums = np.arange(10000), path = './', SN = str(), units = 'Pa', bitweight = np.NaN, bitweight_V = np.NaN, bitweight_Pa = np.NaN, alloutput = False, verbose = True, requireGPS = False, time_adjustment = 0, network = '', station = '', location = '', output_int32 = False):
    emptyGPS = pd.DataFrame.from_dict({'year':np.array([]), 
                                      'date':np.array([]), 
                                      'lat':np.array([]), 
                                      'lon':np.array([]), 
                                      't':np.array([])})
    if(len(station) == 0):
        station = SN
    L = gemlogR.ReadGem([float(x) for x in nums], path, SN, units, float(bitweight), float(bitweight_V), float(bitweight_Pa), alloutput, verbose, requireGPS, network = '', station = '', location = '')
    #pdb.set_trace()
    dataGood = True
    timingGood = True
    ## verify that ReadGem output is good
    if(type(L) == rpy2.rinterface.NULLType): #if it's null, flag it
        dataGood = False
        timingGood = False
    elif((len(L[0]) == 0) | np.isnan(np.sum(L[0]))): # if it's missing GPS data, flag it
        timingGood = False
        LI = L
    else:
        ## try interpolating the time, which may fail
        try:    
            LI = gemlogR.InterpTime(L)
        except:
            timingGood = False
            LI = L
        
    #pdb.set_trace()
    if((not dataGood) | ((not timingGood) & requireGPS)): # if no timing info, return nothing
        data = np.array([])
        if(output_int32):
            data = np.array(data.round(), dtype = 'int32') ## apparently int32 is needed for steim1
        tr = obspy.Trace(data)
        tr.stats.station = station
        tr.stats.location = location # this may well be ''
        tr.stats.channel = 'HDF' # Gem is always HDF
        tr.stats.delta = 0.01
        tr.stats.network = network # can be '' for now and set later
        tr.stats.starttime = obspy.core.UTCDateTime(0)

        gps = emptyGPS
        metadata = pd.DataFrame.from_dict({'millis':[], 'maxWriteTime':[], 'minFifoFree':[], 
                                           'maxFifoUsed':[], 'maxOverruns':[], 'gpsOnFlag':[],
                                           'unusedStack1':[], 'unusedStackIdle':[], 't':[]})
        output = {'data': tr,
                  'metadata': metadata,
                  'gps': gps,
                  'header': pd.DataFrame.from_dict({'SN':[SN],'bitweight_Pa':[np.NaN], 'bitweight_V':[np.NaN]})
                  }   
        return output
    
    header1=pd.DataFrame.from_dict({ key : np.asarray(L[2].rx2(key)) for key in L[2].names[0:-1] }) # normal header
    header2=pd.DataFrame.from_dict({ key : np.asarray(L[2][-1].rx2(key)) for key in L[2][-1].names }) # config
    header = pd.concat([header1, header2], axis=1, sort=False)
          
    metadata = pd.DataFrame.from_dict({ key : np.asarray(L[3].rx2(key)) for key in L[3].names })
    metadata.millis = metadata.millis.apply(int)
    metadata.maxWriteTime = metadata.maxWriteTime.apply(int)
    metadata.minFifoFree = metadata.minFifoFree.apply(int)
    metadata.maxFifoUsed = metadata.maxFifoUsed.apply(int)
    metadata.maxOverruns = metadata.maxOverruns.apply(int)
    metadata.gpsOnFlag = metadata.gpsOnFlag.apply(int)
    metadata.unusedStack1 = metadata.unusedStack1.apply(int)
    metadata.unusedStackIdle=metadata.unusedStackIdle.apply(int)
    
    if(timingGood):
        metadata.t=metadata.t.apply(obspy.core.UTCDateTime)
        gps = pd.DataFrame.from_dict({ key : np.asarray(L[4].rx2(key)) for key in L[4].names })
        gps=gps.dropna() # ignore rows containing NaNs to avoid conversion errors
        gps.year = gps.year.apply(int)
        year = gps.year
        hour = (24*(gps.date % 1))
        minute = (60*(hour % 1))
        second = (60*(minute % 1))
        ts=year.apply(str) + ' ' + gps.date.apply(int).apply(str) + ' ' + hour.apply(int).apply(str) + ':' + minute.apply(int).apply(str) + ':' + second.apply(int).apply(str)
        t = pd.Series(ts.apply(obspy.core.UTCDateTime.strptime, format='%Y %j %H:%M:%S') + (second % 1))
        t.name='t'
        gps = gps.join(t)
    else:
        metadata.t = (metadata.millis/1000).apply(obspy.core.UTCDateTime)
        gps = emptyGPS
        
    data = np.array(LI[1]) # p
    if(output_int32):
        data = np.array(data.round(), dtype = 'int32') ## apparently int32 is needed for steim1
    tr = obspy.Trace(data)
    tr.stats.delta = 0.01
    tr.stats.network = network # can be '' for now and set later
    if(timingGood):
        tr.stats.starttime = LI[0][0] + time_adjustment
    else:
        tr.stats.starttime = obspy.core.UTCDateTime(0)

    if(len(station) == 0): # have to have a station; assume SN if not given
        print(header.SN)
        wsn = 0
        while(True):
            tr.stats.station = header.SN[wsn]
            wsn = wsn + 1
            if((wsn >= len(header.SN)) | (len(tr.stats.station) >= 3)):
                break
    else:
        tr.stats.station = station
    tr.stats.location = location # this may well be ''
    tr.stats.channel = 'HDF' # Gem is always HDF

    output = {'data': tr,
              'metadata': metadata,
              'gps': gps,
              'header': header
    }
    return output

def NewGemVar():
    tr = obspy.Trace()
    tr.stats.delta = 0.01
    gps = pd.DataFrame(columns=['year', 'date', 'lat', 'lon'])
    metadata = pd.DataFrame(columns=['millis', 'batt', 'temp', 'A2', 'A3', \
                                     'maxWriteTime', 'minFifoFree', 'maxFifoUsed', \
                                     'maxOverruns', 'gpsOnFlag', 'unusedStack1',\
                                     'unusedStackIdle', 't'])
    output = {'data': tr,
              'metadata': metadata,
              'gps': gps
    }
    return output


def MakeDB(path, pattern = '*', savefile = './DB.csv'):
    #path = 'mseed'
    #pattern = '*'
    files = glob.glob(path + '/' + pattern)
    files.sort()
    DB = []
    count = 0
    for file in files:
        tr = obspy.read(file)[0]
        maxVal = tr.data.max()
        minVal = tr.data.min()
        tr.detrend('linear')
        tr.filter('highpass', freq=0.5)
        amp_HP = tr.std()
        row = pd.DataFrame([[file, tr.stats.station, tr.stats.location, amp_HP, maxVal, minVal, tr.stats.starttime, tr.stats.endtime]], columns = ['filename', 'station', 'location', 'amp_HP', 'max', 'min', 't1', 't2'])
        DB.append(row)
        if((count % 100) == 0):
            print(str(count) + ' of ' + str(len(files)))
        count = count + 1
    DB = pd.concat(DB)
    DB.to_csv(savefile)
    return(DB)

def CalcStationStats(DB, t1, t2):
    import obspy, glob
    import pandas as pd
    from obspy import UTCDateTime as T
    #t1 = '2020-04-14'
    #t2 = '2020-04-24T20:00:00'
    t1 = obspy.core.UTCDateTime(t1)
    t2 = obspy.core.UTCDateTime(t2)
    numHour = (t2 - t1)/3600.0
    DB.t1 = DB.t1.apply(T)
    DB.t2 = DB.t2.apply(T)
    DB.goodData = (DB.amp_HP > 0.5) & (DB.amp_HP < 2e4) & ((DB.t2 - DB.t1) > 3598) & ((DB.t2 - DB.t1) < 3602)
    DB.anyData = (DB.amp_HP > 0) 
    out = []
    for sta in DB.station.unique():
        w = np.where((DB.station == sta) & (DB.t1 > t1) & (DB.t2 < t2))[0]
        if(len(w) == 0):
            continue
        else:
            q1 = np.quantile(np.array(DB.amp_HP)[w], 0.25)
            q3 = np.quantile(np.array(DB.amp_HP)[w], 0.75)
            out.append(pd.DataFrame([[sta, np.sum(np.array(DB.goodData)[w])/numHour, np.sum(np.array(DB.anyData)[w])/numHour, q1, q3]], columns = ['station', 'goodData', 'anyData', 'q1', 'q3']))
    out = pd.concat(out)
    return(out)


## 55 (3.03), 84 (4.37), 108 (2.04), 49 (1.78), others (1.3-1.6)

#L55=gemlog.ReadGemPy(nums=np.arange(6145,6151),SN='055', path = 'raw')
#import matplotlib.pyplot as plt
def PlotAmp(DB):
    allSta = DB.station.unique()
    allSta.sort()
    for sta in DB.station.unique():
        w = np.where(DB.station == sta)[0]
        w.sort()
        plt.plot(DB.t1[w], np.log10(DB.amp_HP[w]), '.')
        print(str(sta) + ' ' + str(np.quantile(DB.amp_HP[w], 0.25)))
    plt.legend(allSta)
    plt.show()

################################################
def ReadSN(fn):
    SN_line = pd.read_csv(fn, delimiter = ',', skiprows = 4, nrows=1, dtype = 'str', names=['s', 'SN'])
    SN = SN_line['SN'][0]
    return SN

def ReadVersion(fn):
    versionLine = pd.read_csv(fn, delimiter = ',', nrows=1, dtype = 'str', names=['s'])
    version = versionLine['s'][0][7:]
    return version
    
def ReadConfig(fn):
    config = pd.Series({'gps_mode': 1,
                    'gps_cycle' : 15,
                    'gps_quota' : 20,
                    'adc_range' : 0,
                    'led_shutoff' : 0,
                    'serial_output' : 0}) ## default config: it's fairly safe to use this as the default because any other configuration would require ...
    for j in range(10):
        line = pd.read_csv(fn, skiprows = j+1, nrows=1, delimiter = ',', dtype = 'str', names = ['na', 'gps_mode','gps_cycle','gps_quota','adc_range','led_shutoff','serial_output'])
        if line.iloc[0,0] == 'C':
            #config = line.iloc[0,1:]
            config = {key:int(line[key]) for key in list(line.keys())[1:]}
            break
    return config

def fn2nums(fn_list):
    nums = []
    for i, fn in enumerate(fn_list):
        nums[i] = int(fn[-8:-5])
    return nums


def FindRightFiles(path, SN, nums):
    ## list all Gem files in the path
    fnList = glob.glob(path + '/' + 'FILE[0-9][0-9][0-9][0-9].[0-9][0-9][0-9]')
    fnList.sort()
    fnList = np.array(fnList)
    
    ## find out what all the files' SNs are
    ext = np.array([x[-3:] for x in fnList])
    for i in range(len(ext)):
        if ext[i] == 'TXT':
            ext[i] = ReadSN(fnList[i])
    ## check the files for SN and num
    goodFnList = []
    for i, fn in enumerate(fnList):
        fnNum = int(fn[-8:-4])
        fnSN = ext[i]
        if (fnNum in nums) & (fnSN == SN):
            goodFnList.append(fn)
    if len(goodFnList) == 0:
        print('No good data files found for specified nums and SN ' + SN)
        return []
        ## fix this to be an exception or warning?
    ## make sure they aren't empty
    goodNonemptyFnList = []
    for fn in goodFnList:
        if os.path.getsize(fn) > 0:
            goodNonemptyFnList.append(fn)
    if(len(goodNonemptyFnList) == 0):
        ## warning
        print('No non-empty files')
        return []
    if(len(goodNonemptyFnList) < len(goodFnList)):
        print('Some files are empty, skipping')
    return goodNonemptyFnList    



def UnwrapMillis(new, old, rollover = 2**13):
    return old + ((new - (old % rollover) + rollover/2) % rollover) - rollover/2

def CheckGPS(line): # return True if GPS line is good
    #G,msPPS,msLag,yr,mo,day,hr,min,sec,lat,lon
    return not ((line[8] == 0) or (line[8] > 90) or (line[8] < -90) or # lat
                (line[9] == 0) or (line[9] > 180) or (line[9] < -180) or # lon
                (line[1] > 1000) or (line[1] < 0) or # lag
                (line[2] > 2040) or (line[2] < 2014) or # year
                (line[3] > 12) or (line[3] < 1) or # month
                (line[4] > 31) or (line[4] < 1) or # day
                (line[5] > 24) or (line[5] < 0) or # hour
                (line[6] > 60) or (line[6] < 0) or # minute
                (line[7]>60) or (line[7]<0) or (line[7]!=np.round(line[7]))) # second


def MakeGPSTime(line):
    return obspy.UTCDateTime(int(line[2]), int(line[3]), int(line[4]), int(line[5]), int(line[6]), int(line[7]))

def MillisToTime(G):
    coefficients = np.polyfit(G.msPPS, G.t, 3)    
    #print(coefficients)
    pf = np.poly1d(coefficients)
    return pf

def ReadGem_v0_9_single(fn, startMillis):
    ## pre-allocate the arrays (more space than is needed)
    M = np.ndarray([15000,12]) # no more than 14400
    G = np.ndarray([15000,11]) # no more than 14400
    D = np.ndarray([750000,2]) # expected number 7.2e5
    d_index = 0
    m_index = 0
    g_index = 0
    millis = startMillis
    ## open the file for reading
    with open(fn, 'r', newline = '', encoding='ascii', errors = 'ignore') as csvfile:
        lines = csv.reader(csvfile, delimiter = ',')
        i = 0
        for line in lines:
            ## determine the line type, and skip if it's not necessary data (e.g. debugging info)
            lineType = line[0][0]
            if not (lineType in ['D', 'G', 'M']):
                continue
            ## remove the line type ID and make into a nice array
            if lineType == 'D':
                line[0] = line[0][1:]
            else:
                line = line[1:]
            line = np.array([float(x) for x in line])    
            ## unwrap the millis count (always first element of line)
            millis = UnwrapMillis(line[0], millis)
            line[0] = millis
            ## write the line to its matrix
            if lineType == 'D':
                D[d_index,:] = line
                d_index += 1
            elif lineType == 'M':
                M[m_index,:] = line
                m_index += 1
            elif (lineType == 'G') and CheckGPS(line):
                G[g_index,:10] = line
                G[g_index,10] = MakeGPSTime(line)
                g_index += 1
    #pdb.set_trace()
    ## remove unused space in pre-allocated arrays
    D = D[:d_index,:]
    G = pd.DataFrame(G[:g_index,:], columns = ['msPPS', 'msLag', 'year', 'month', 'day', 'hour', 'minute', 'second', 'lat', 'lon', 't'])
    M = pd.DataFrame(M[:m_index,:], columns = ['millis', 'batt', 'temp', 'A2', 'A3', \
                                               'maxWriteTime', 'minFifoFree', 'maxFifoUsed', \
                                               'maxOverruns', 'gpsOnFlag', 'unusedStack1',\
                                               'unusedStackIdle'])
    ## process data (version-dependent)
    D[:,1] = D[:,1].cumsum()
    return {'data': D, 'metadata': M, 'gps': G}

def ReadGem(nums = np.arange(10000), path = './', SN = '', units = 'Pa', bitweight = np.NaN, bitweight_V = np.NaN, bitweight_Pa = np.NaN, verbose = True, network = '', station = '', location = ''):
    if(len(station) == 0):
        station = SN
    ## add asserts, especially for SN
    fnList = FindRightFiles(path, SN, nums)
    version = ReadVersion(fnList[0])
    config = ReadConfig(fnList[0])
    if version == '0.9':
        L = ReadGem_v0_9(fnList)
    elif version == '0.85C':
        L = ReadGem_v0_9(fnList) # will the same function work for both?
    #L['header'] = MakeHeader(L, config)
    f = MillisToTime(L['gps'])
    M = L['metadata']
    D = L['data']
    G = ReformatGPS(L['gps'])
    M['t'] = f(M['millis'])
    D = np.hstack((D, f(D[:,0]).reshape([D.shape[0],1])))
    header = L['header']
    header.SN = SN
    header.t1 = f(header.t1)
    header.t2 = f(header.t2)
    
    ## interpolate data to equal spacing to make obspy trace
    tr = InterpTime(D) # populates known fields: channel, delta, and starttime
    ## populate the rest of the trace stats
    tr.stats.station = station
    tr.stats.location = location # this may well be ''
    tr.stats.network = network # can be '' for now and set later
    ## add bitweight and config info to header
    bitweight_info = GetBitweightInfo(SN, config, units)
    #pdb.set_trace()
    for key in bitweight_info.keys():
        header[key] = bitweight_info[key]
    for key in config.keys():
        header[key] = config[key]
    header['file_format_version'] = version
    return {'data': tr, 'metadata': M, 'gps': G, 'header' : header}


def ReadGem_v0_9(fnList):
    ## initialize the output variables
    G = pd.DataFrame(columns = ['msPPS', 'msLag', 'year', 'month', 'day', 'hour', 'minute', 'second', 'lat', 'lon', 't'])
    M = pd.DataFrame(columns = ['millis', 'batt', 'temp', 'A2', 'A3', \
                                               'maxWriteTime', 'minFifoFree', 'maxFifoUsed', \
                                               'maxOverruns', 'gpsOnFlag', 'unusedStack1',\
                                               'unusedStackIdle'])
    D = np.ndarray([0,2]) # expected number 7.2e5
    num_filler = np.zeros(len(fnList))
    header = pd.DataFrame.from_dict({'file': fnList,
                                     'SN':['' for fn in fnList],
                                     'lat': num_filler,
                                     'lon': num_filler,
                                     't1': num_filler,
                                     't2': num_filler
                                     })
    ## loop through the files
    startMillis = 0
    for i,fn in enumerate(fnList):
        print('File ' + str(i+1) + ' of ' + str(len(fnList)) + ': ' + fn)
        L = ReadGem_v0_9_single(fn, startMillis)
        M = pd.concat((M, L['metadata']))
        G = pd.concat((G, L['gps']))
        D = np.vstack((D, L['data']))
        startMillis = D[-1,0]
        header.loc[i,'lat'] = np.median(L['gps']['lat'])
        header.loc[i,'lon'] = np.median(L['gps']['lon'])
        header.loc[i, 't1'] = L['data'][0,0] # save this as a millis first, then convert
        header.loc[i,'t2'] = L['data'][-1,0]
    return {'metadata':M, 'gps':G, 'data': D, 'header': header}

#x = ReadGem_v0_9(fnList[:1])

#start = time.clock()
#x=ReadGem_v0_9_single(fn[0])
#time.clock() - start
















#########################################################
def InterpTime(data, t1 = -np.Inf, t2 = np.Inf):
    eps = 0.001 # 1 ms ## 2019-09-11: formerly 1 ms, but this caused the first second after midnight UTC to be skipped...nearly all midnight converted data are therefore of the form ??.???.00.00.01.???.0. Changing it to 0.01 (1 sample) should fix this. Bug dates back to at least 2016-11-10!
    
    ## break up the data into continuous chunks, then round off the starts to the appropriate unit
    ## t1 is the first output sample; should be first integer second after or including the first sample (ceiling)
    t_in = data[:,2]
    p_in = data[:,1]
    #pdb.set_trace()
    t1 = np.trunc(t_in[t_in >= t1][0]+1-0.01) ## 2019-09-11
    t2 = t_in[t_in <= (t2 + .01 + eps)][-1] # add a sample because t2 is 1 sample before the hour
    ## R code here had code to catch t2 <= t1. should add that.
    breaks_raw = np.where(np.diff(t_in) > 0.015)[0]
    breaks = breaks_raw[(t_in[breaks_raw] > t1) & (t_in[breaks_raw+1] < t2)]
    starts = np.hstack([t1, t_in[breaks+1]]) # start times of continuous chunks
    ends = np.hstack([t_in[breaks], t2]) # end times of continuous chunks
    w_same = (starts != ends)
    starts = starts[w_same]
    ends = ends[w_same]
    starts_round = np.trunc(starts)
    ends_round = np.trunc(ends+eps+1)
    ## make an output time vector excluding data gaps, rounded to the nearest samples
    t_interp = np.zeros(0)
    for i in range(len(starts_round)):
        t_new = np.arange(starts_round[i], ends_round[i] + eps, 0.01)
        t_interp = np.concatenate([t_interp, t_new])
    t_interp = t_interp[(t_interp >= (t1-eps)) & (t_interp < (t2 + eps))]
    ## interpolate to find pressure at these sample times
    f = scipy.interpolate.CubicSpline(t_in, p_in)
    p_interp = np.array(f(t_interp).round(), dtype = 'int32')
    tr = obspy.Trace(p_interp)
    tr.stats.starttime = t_interp[0]
    tr.stats.delta = 0.01
    tr.stats.channel = 'HDF'
    return tr

###############################################################
#  version bitweight_Pa  bitweight_V min_SN max_SN
#1    0.50  0.003256538 7.362894e-08      3      7
#2    0.70  0.003256538 7.362894e-08      8     14
#3    0.80  0.003256538 7.362894e-08     15     19
#4    0.82  0.003256538 7.362894e-08     20     37
#5    0.90  0.003543324 7.362894e-08     38     40
#6    0.91  0.003543324 7.362894e-08     41     43
#7    0.92  0.003543324 7.362894e-08     44     46
#8    0.98  0.003501200 7.275362e-08     47     49
#9    0.99  0.003501200 7.275362e-08     50    54
#10    0.991  0.003501200 7.275362e-08     52    54
#11    0.992  0.003501200 7.275362e-08     55    57
#12    1.00  0.003501200 7.275362e-08     58    107
#13    1.00  0.003501200 7.275362e-08     108    Inf
#'bitweight_Pa': [0.003256538, 0.003256538, 0.003256538, 0.003256538, 0.003543324, 0.003543324, 0.003543324, 0.003501200, 0.003501200, 0.003501200, 0.003501200, 0.003501200, 0.003501200],

#bitweight_V = [7.362894e-08, 7.362894e-08, 7.362894e-08, 7.362894e-08, 7.362894e-08, 7.362894e-08, 7.362894e-08, 7.275362e-08, 7.275362e-08, 7.275362e-08, 7.275362e-08,7.275362e-08,7.275362e-08]
__AVCC__ = np.array([3.373, 3.373,  3.373,  3.373,  3.373,  3.373,  3.373, 7.275362e-08, 7.275362e-08, 7.275362e-08, 7.275362e-08, 7.275362e-08, 7.275362e-08]),
def __AVCC__(version):
    if version >= 0.90:
        return 3.1
    else:
        return 3.373

def __gain__(version):
    if version >= 0.98:
        return 1 + 50/0.470
    else:
        return 1 + 49.4/0.470

def GemSpecs(SN):
    versionTable = {'version': np.array([0.5, 0.7, 0.8, 0.82, 0.9, 0.91, 0.92, 0.98, 0.99, 0.991, 0.992, 1, 1.01]),
                    'min_SN': np.array([3, 8, 15, 20, 38, 41, 44, 47, 50, 52, 55, 58, 108]),
                    'max_SN': np.array([7, 14, 19, 37, 40, 43, 46, 49, 51, 54, 57, 107, np.Inf])
    }
    version = versionTable['version'][(int(SN) >= versionTable['min_SN']) & (int(SN) <= versionTable['max_SN'])][0]
    bitweight_V = 0.256/2**15/__gain__(version)
    sensitivity = __AVCC__(version)/7.0 * 45.13e-6 # 45.13 uV/Pa is with 7V reference from Marcillo et al., 2012
    return { 'version': version,
             'bitweight_V': bitweight_V,
             'bitweight_Pa': bitweight_V/sensitivity
    }


## Eventually, GetBitweightInfo should perform all the functions of the R gemlog equivalent...but not yet.
def GetBitweightInfo(SN, config, units = 'Pa'):
    if config['adc_range'] == 0: # high gain
        multiplier = 1
    elif config['adc_range'] == 1: # low gain
        multiplier = 2
    else:
        #pdb.set_trace()
        raise BaseException('Invalid Configuration')
    specs = GemSpecs(SN)
    specs['bitweight_Pa'] *= multiplier
    specs['bitweight_V'] *= multiplier
    if units.upper() == 'PA':
        specs['bitweight'] = specs['bitweight_Pa']
    elif units.upper() == 'V':
        specs['bitweight'] = specs['bitweight_V']
    elif units.lower() == 'counts':
        specs['bitweight'] = 1
    else:
        raise BaseException('Invalid Units')
    return specs

def ReformatGPS(G_in):
    t = [obspy.UTCDateTime(tt) for tt in G_in.t]
    date = [tt.julday + tt.hour/24.0 + tt.minute/1440.0 + tt.second/86400.0 for tt in t]
    G_dict = {'year': [int(year) for year in G_in.year],
              'date': date,
              'lat': G_in.lat,
              'lon': G_in.lon,
              't': t}
    
    return pd.DataFrame.from_dict(G_dict)
