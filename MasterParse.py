import csv
import sys
from operator import itemgetter, attrgetter
import os
#from collections import OrderedDict
import itertools

try:
    from itertools import izip_longest  # added in Py 2.6
except ImportError:
    from itertools import zip_longest as izip_longest  # name change in Py 3.x

try:
    from itertools import accumulate  # added in Py 3.2
except ImportError:
    def accumulate(iterable):
        'Return running totals (simplified version).'
        total = next(iterable)
        yield total
        for value in iterable:
            total += value
            yield total

###############################
# Utility Functions
###############################

# Function to Create Parsing Specifications
def make_parser(fieldwidths):
    cuts = tuple(cut for cut in accumulate(abs(fw) for fw in fieldwidths))
    pads = tuple(fw < 0 for fw in fieldwidths) # bool values for padding fields
    flds = tuple(izip_longest(pads, (0,)+cuts, cuts))[:-1]  # ignore final one
    parse = lambda line: tuple(line[i:j] for pad, i, j in flds if not pad)
    # optional informational function attributes
    parse.size = sum(abs(fw) for fw in fieldwidths)
    parse.fmtstring = ' '.join('{}{}'.format(abs(fw), 'x' if fw < 0 else 's')
                                                for fw in fieldwidths)
    return parse

# Function to Prepare String for DB
def stripStr(string):
    return string.lstrip().rstrip()

###############################
# Main Logic Function
###############################
def parseMaster(filename):
# Open Document For Reading
    try:
        f = open(filename,'r+')
    except(OSError, IOError) as e:
        print('Fail: '+str(e))
        sys.exit(0)

    # Get Header
    header = next(f)

    # Validate Header
    if ((header.lstrip().rstrip() != '/XI20') and (header.lstrip().rstrip() != '/XI40')):
        raise Exception('Expected File Header Not Found')

    # Create Parser Remove Pad Fields
    fieldwidths = (20,20,20,20,20,20,2,8,20,2,-1,1,2,10,
                   3,2,2,6,1,6,1,10,1,40,20,12,-1,4,10,10,
                   12,10,10,10,10,10,1,1,10,1,10,-40,14,6,
                   20,2,2,15,1,2,2,2,20,20,4,1,1,4,2,4,8,
                   8,12,12,5,1,2,2,2,3,-23,10,20,4,4,10,3,
                   1,14,14,20,8,1,4,1,5,2,2,2,1,1,10,1,3,
                   -17,14,-2,5,12,6,30,30,12,6,5,1,1,2,3,14,
                   14,1,1,14,-6,6,12,20,20,2,3,2,1,2,1,10,
                   1,1,1,10,10,10,10,10,6,10,10,10,10,3,
                   2,10,3,3,1,1,1,-1,2,6,1,1,2,2,10,10,10,
                   10,10,10,1,1,10,1,1,10,10,2,2,12,10,1,
                   4,8,1,6,8,4,4,4,4,8,4,4,4,20,1,6,-5,8,
                   1,2,4,4,10,1,2,1,2,4,10,1,1,1,1,1,36,
                   36,3,-14,1,2,4,10,1,1,2,4,10,1,1,2,4,
                   10,1,-13,-2)
    parse = make_parser(fieldwidths)

    # Create Container Dicts of 3 types of master records (recorder, customer, channel)
    recorderList = []
    customerList = []
    channelList = []

    # Create Container for all Master Records for Return
    masterList = []

    # Go Through All Lines in Document
    j = 0
    lines = f.readlines()
    for line in lines:
        fields = parse(line)

        # Create Dict to Contain MF Records
        dCM = dict()
        dMM = dict()
        dRM = dict()

        # Assign every pasrsed field to corresponding dict descriptor
        dCM['CM_A1AUTOPLD']=stripStr(fields[186])
        dCM['CM_A1CYCLE']=stripStr(fields[187])
        dCM['CM_A1ENDTIME']=stripStr(fields[184])
        dCM['CM_A1MFPEND']=stripStr(fields[192])
        dCM['CM_A1UPLDATE']=stripStr(fields[185])
        dCM['CM_A2AUTOUPLD']=stripStr(fields[188])
        dCM['CM_A2CYCLE']=stripStr(fields[189])
        dCM['CM_A2ENDTIME']=stripStr(fields[190])
        dCM['CM_A2MFPEND']=stripStr(fields[193])
        dCM['CM_A2UPLDATE']=stripStr(fields[191])
        dCM['CM_A3AUTOUPLD']=stripStr(fields[200])
        dCM['CM_A3CYCLE']=stripStr(fields[201])
        dCM['CM_A3ENDTIME']=stripStr(fields[202])
        dCM['CM_A3MFPEND']=stripStr(fields[204])
        dCM['CM_A3UPLDATE']=stripStr(fields[203])
        dCM['CM_A4AUTOUPLD']=stripStr(fields[205])
        dCM['CM_A4CYCLE']=stripStr(fields[206])
        dCM['CM_A4ENDTIME']=stripStr(fields[207])
        dCM['CM_A4MFPEND']=stripStr(fields[209])
        dCM['CM_A4UPLDATE']=stripStr(fields[208])
        dCM['CM_A5AUTOPLD']=stripStr(fields[210])
        dCM['CM_A5CYCLE']=stripStr(fields[211])
        dCM['CM_A5ENDTIME']=stripStr(fields[212])
        dCM['CM_A5MFPEND']=stripStr(fields[214])
        dCM['CM_A5UPLDATE']=stripStr(fields[213])
        dCM['CM_ACCOUNT']=stripStr(fields[8])
        dCM['CM_ADDR1']=stripStr(fields[2])
        dCM['CM_ADDR2']=stripStr(fields[3])
        dCM['CM_APPLCODE']=stripStr(fields[15])
        dCM['CM_AUTOUPLD']=stripStr(fields[17])
        dCM['CM_BBSLIB']=stripStr(fields[180])
        dCM['CM_BILLKVAR']=stripStr(fields[34])
        dCM['CM_CHDATE']=stripStr(fields[20])
        dCM['CM_CHTYPE']=stripStr(fields[21])
        dCM['CM_COMMENT']=stripStr(fields[22])
        dCM['CM_CONTACT']=stripStr(fields[4])
        dCM['CM_CRITPROG']=stripStr(fields[38])
        dCM['CM_CUSTID']=stripStr(fields[0])
        dCM['CM_CYCLE']=stripStr(fields[9])
        dCM['CM_DL1']=stripStr(fields[29])
        dCM['CM_DL2']=stripStr(fields[30])
        dCM['CM_ENDTIME']=stripStr(fields[25])
        dCM['CM_FSLGLD']=stripStr(fields[36])
        dCM['CM_GROUP']=stripStr(fields[16])
        dCM['CM_LCTYPE']=stripStr(fields[35])
        dCM['CM_LOGCHANS']=stripStr(fields[13])
        dCM['CM_MASTMTRID']=stripStr(fields[24])
        dCM['CM_MASTSCHED']=stripStr(fields[14])
        dCM['CM_MFPEND']=stripStr(fields[19])
        dCM['CM_MULREC']=stripStr(fields[10])
        dCM['CM_NAME']=stripStr(fields[1])
        dCM['CM_OPTDATA']=stripStr(fields[37])
        dCM['CM_PBS']=stripStr(fields[181])
        dCM['CM_PF1']=stripStr(fields[31])
        dCM['CM_PF2']=stripStr(fields[32])
        dCM['CM_PFDL']=stripStr(fields[33])
        dCM['CM_PHONE']=stripStr(fields[5])
        dCM['CM_PRCUSTID']=stripStr(fields[23])
        dCM['CM_RATE']=stripStr(fields[18])
        dCM['CM_RPCYCLE']=stripStr(fields[182])
        dCM['CM_RPENDTIME']=stripStr(fields[183])
        dCM['CM_RPTFILE']=stripStr(fields[28])
        dCM['CM_RPTSTART']=stripStr(fields[26])
        dCM['CM_RPTSTOP']=stripStr(fields[27])
        dCM['CM_SIC']=stripStr(fields[7])
        dCM['CM_STRATA']=stripStr(fields[11])
        dCM['CM_TOUCODE']=stripStr(fields[6])
        dCM['CM_UPLDATE']=stripStr(fields[12])
        dMM['MM_ABSDIFF']=stripStr(fields[167])
        dMM['MM_CFCODE']=stripStr(fields[133])
        dMM['MM_CFORM']=stripStr(fields[121])
        dMM['MM_CHDATE']=stripStr(fields[154])
        dMM['MM_CHTYPE']=stripStr(fields[155])
        dMM['MM_CORRFACT']=stripStr(fields[165])
        dMM['MM_CUSTID']=stripStr(fields[110])
        dMM['MM_DECPOS']=stripStr(fields[139])
        dMM['MM_ENCBASE']=stripStr(fields[118])
        dMM['MM_ENCTYPE']=stripStr(fields[116])
        dMM['MM_Extra1']=stripStr(fields[194])
        dMM['MM_Extra2']=stripStr(fields[195])
        dMM['MM_Extra3']=stripStr(fields[196])
        dMM['MM_FLOW']=stripStr(fields[153])
        dMM['MM_GROUP']=stripStr(fields[108])
        dMM['MM_KVARH']=stripStr(fields[137])
        dMM['MM_KVASET']=stripStr(fields[144])
        dMM['MM_LASTENC']=stripStr(fields[122])
        dMM['MM_LASTKVR']=stripStr(fields[134])
        dMM['MM_LASTVIS']=stripStr(fields[123])
        dMM['MM_LFTOL']=stripStr(fields[135])
        dMM['MM_LOGCHAN']=stripStr(fields[113])
        dMM['MM_LOSSOPT']=stripStr(fields[145])
        dMM['MM_MAPCHAN']=stripStr(fields[159])
        dMM['MM_MAXINT']=stripStr(fields[128])
        dMM['MM_MAXTOT']=stripStr(fields[130])
        dMM['MM_METERID']=stripStr(fields[109])
        dMM['MM_METINSTD']=stripStr(fields[158])
        dMM['MM_METSEQ']=stripStr(fields[160])
        dMM['MM_MININT']=stripStr(fields[129])
        dMM['MM_MINTOT']=stripStr(fields[131])
        dMM['MM_MMULT']=stripStr(fields[124])
        dMM['MM_MREADS']=stripStr(fields[120])
        dMM['MM_NDIALS']=stripStr(fields[119])
        dMM['MM_NRKDIALS']=stripStr(fields[163])
        dMM['MM_NRKVARH']=stripStr(fields[117])
        dMM['MM_NRKVMULT']=stripStr(fields[162])
        dMM['MM_NRKVSN']=stripStr(fields[161])
        dMM['MM_OMITUPLD']=stripStr(fields[143])
        dMM['MM_OPTDATA']=stripStr(fields[156])
        dMM['MM_PCTCHG']=stripStr(fields[132])
        dMM['MM_PFTOL']=stripStr(fields[136])
        dMM['MM_PHASE']=stripStr(fields[152])
        dMM['MM_PMULT']=stripStr(fields[125])
        dMM['MM_POFFS']=stripStr(fields[126])
        dMM['MM_PTRATIO']=stripStr(fields[141])
        dMM['MM_PYSCHAN']=stripStr(fields[112])
        dMM['MM_RDCHAN']=stripStr(fields[140])
        dMM['MM_RDTOL']=stripStr(fields[151])
        dMM['MM_RECID']=stripStr(fields[107])
        dMM['MM_REGTYPE']=stripStr(fields[115])
        dMM['MM_SERVTYPE']=stripStr(fields[142])
        dMM['MM_TOLPCT']=stripStr(fields[127])
        dMM['MM_TOLTYPE']=stripStr(fields[138])
        dMM['MM_TOTPULSE']=stripStr(fields[157])
        dMM['MM_UMCODE']=stripStr(fields[114])
        dMM['MM_UOMSCALE']=stripStr(fields[105])
        dMM['MM_VCLC']=stripStr(fields[149])
        dMM['MM_VILC']=stripStr(fields[148])
        dMM['MM_VOLTS']=stripStr(fields[150])
        dMM['MM_WCLC']=stripStr(fields[147])
        dMM['MM_WILC']=stripStr(fields[146])
        dMM['MM_XFACCT']=stripStr(fields[111])
        dMM['MM_ZEROINT']=stripStr(fields[164])
        dRM['RM_ CORRECTIONFACTORTOL']=stripStr(fields[199])
        dRM['RM_ INBOUNDCALLDELAY1']=stripStr(fields[169])
        dRM['RM_ INBOUNDCALLDELAY2']=stripStr(fields[170])
        dRM['RM_ MTRTYPE']=stripStr(fields[166])
        dRM['RM_ SERVICE_BASED_ID']=stripStr(fields[177])
        dRM['RM_ALARM']=stripStr(fields[91])
        dRM['RM_ANSWINDOW']=stripStr(fields[57])
        dRM['RM_AWDAYS']=stripStr(fields[53])
        dRM['RM_BATINSTD']=stripStr(fields[93])
        dRM['RM_BATLOGD']=stripStr(fields[69])
        dRM['RM_BATLOGM']=stripStr(fields[70])
        dRM['RM_BAUD']=stripStr(fields[51])
        dRM['RM_BAUD1']=stripStr(fields[62])
        dRM['RM_CALLMODE']=stripStr(fields[48])
        dRM['RM_CALLTIME']=stripStr(fields[79])
        dRM['RM_CFGNAME']=stripStr(fields[96])
        dRM['RM_CHAN']=stripStr(fields[43])
        dRM['RM_CHDATE']=stripStr(fields[87])
        dRM['RM_CHOFFSET']=stripStr(fields[66])
        dRM['RM_CHROM_ID']=stripStr(fields[104])
        dRM['RM_CHTYPE']=stripStr(fields[88])
        dRM['RM_CONNTYPE']=stripStr(fields[168])
        dRM['RM_CURINTVLEN']=stripStr(fields[174])
        dRM['RM_DCHAIN']=stripStr(fields[73])
        dRM['RM_DCMASTER']=stripStr(fields[74])
        dRM['RM_DCSLAVE']=stripStr(fields[75])
        dRM['RM_DEVID']=stripStr(fields[76])
        dRM['RM_DEVSN']=stripStr(fields[77])
        dRM['RM_DEVTYP']=stripStr(fields[44])
        dRM['RM_DST']=stripStr(fields[85])
        dRM['RM_EXPANDED']=stripStr(fields[178])
        dRM['RM_FWDCALL']=stripStr(fields[86])
        dRM['RM_GROUP']=stripStr(fields[40])
        dRM['RM_HOMEPHONE1']=stripStr(fields[197])
        dRM['RM_HOMEPHONE2']=stripStr(fields[198])
        dRM['RM_INITDATE']=stripStr(fields[67])
        dRM['RM_INPHR']=stripStr(fields[42])
        dRM['RM_INPUTDESC']=stripStr(fields[94])
        dRM['RM_INSCID']=stripStr(fields[173])
        dRM['RM_IP_PORT']=stripStr(fields[179])
        dRM['RM_LASTTIME']=stripStr(fields[71])
        dRM['RM_LMTCALL']=stripStr(fields[54])
        dRM['RM_LOCALPW']=stripStr(fields[68])
        dRM['RM_LOCAT']=stripStr(fields[41])
        dRM['RM_LOP']=stripStr(fields[78])
        dRM['RM_LPCPHONE']=stripStr(fields[49])
        dRM['RM_MEMSIZE']=stripStr(fields[72])
        dRM['RM_METERCGF']=stripStr(fields[58])
        dRM['RM_METERCHG']=stripStr(fields[80])
        dRM['RM_METLOC']=stripStr(fields[64])
        dRM['RM_MFILE']=stripStr(fields[92])
        dRM['RM_NRINGS']=stripStr(fields[47])
        dRM['RM_NUMCALLS']=stripStr(fields[171])
        dRM['RM_NUMTRIES']=stripStr(fields[172])
        dRM['RM_OLDDST']=stripStr(fields[106])
        dRM['RM_PLUG_MISS']=stripStr(fields[101])
        dRM['RM_PLUG_OPT']=stripStr(fields[99])
        dRM['RM_PLUG_OUT']=stripStr(fields[100])
        dRM['RM_PLUG_PER']=stripStr(fields[102])
        dRM['RM_PLUG_REF']=stripStr(fields[103])
        dRM['RM_PW1']=stripStr(fields[59])
        dRM['RM_PW2']=stripStr(fields[60])
        dRM['RM_RDRECID']=stripStr(fields[90])
        dRM['RM_READINS']=stripStr(fields[65])
        dRM['RM_RECID']=stripStr(fields[39])
        dRM['RM_RELAYDESC']=stripStr(fields[95])
        dRM['RM_RELAYTYPE']=stripStr(fields[81])
        dRM['RM_RIPEND']=stripStr(fields[45])
        dRM['RM_ROUTE']=stripStr(fields[97])
        dRM['RM_RPC']=stripStr(fields[83])
        dRM['RM_RPCPHONE']=stripStr(fields[50])
        dRM['RM_SEQROUTE']=stripStr(fields[98])
        dRM['RM_SLOTNUMBER']=stripStr(fields[176])
        dRM['RM_STATUS']=stripStr(fields[82])
        dRM['RM_STATUSINPUTS']=stripStr(fields[61])
        dRM['RM_TIM']=stripStr(fields[46])
        dRM['RM_TIMESET']=stripStr(fields[52])
        dRM['RM_TIMETOL']=stripStr(fields[89])
        dRM['RM_TOTMETERS']=stripStr(fields[84])
        dRM['RM_TZONEADJ']=stripStr(fields[56])
        dRM['RM_UADDR']=stripStr(fields[55])
        dRM['RM_UPDINTVLEN']=stripStr(fields[175])
        dRM['RM_VOLTAGE']=stripStr(fields[63])

        # Add Dicts to List of Dicts
        recorderList.append(dRM)
        customerList.append(dCM)
        channelList.append(dMM)
        

    # Remove Duplicates from recorder and customer dictionaries
    deduped_recorderList = list({dRM["RM_RECID"]: dRM for dRM in recorderList}.values())
    deduped_customerList = list({dCM["CM_CUSTID"]: dCM for dCM in customerList}.values())

    # Add recorders, customers, and channels to master return
    masterList.append(deduped_recorderList)
    masterList.append(deduped_customerList)
    masterList.append(channelList)
    
    # Close the Master File
    f.close()

    return(masterList)

# Standalone Script Section
if __name__ == "__main__":
    fname = r'C:\Users\CJANCSAR\Documents\FNBUG\ALL.DAT'
    ret = parseMaster(fname)
 
