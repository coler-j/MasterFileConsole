import csv
import sys
from operator import itemgetter, attrgetter
import os
from collections import OrderedDict
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
# Input: string with trailing/leading whitespace
# Output: string with no whitespace
def stripStr(string):
    return string.lstrip().rstrip()

# Standalone Script Section
if __name__ == "__main__":

    # Open Document For Reading
    fname = r'C:\Users\CJANCSAR\Documents\FNBUG'
    try:
        f = open(fname,'r+')
    except(OSError, IOError) as e:
        print('Fail: '+str(e))
        sys.exit(0)

    # Get Header
    header = next(f)

    # Validate Header
    if ((header.lstrip().rstrip() != '/XI20') and (header.lstrip().rstrip() != '/XI40')):
        raise Exception('Expected File Header Not Found')

    # Create Parser
    fieldwidths = (20,20,20,20,20,20,2,8,20,2,1,1,2,10,
                   3,2,2,6,1,6,1,10,1,40,20,12,1,4,10,10,
                   12,10,10,10,10,10,1,1,10,1,10,40,14,6,
                   20,2,2,15,1,2,2,2,20,20,4,1,1,4,2,4,8,
                   8,12,12,5,1,2,2,2,3,23,10,20,4,4,10,3,
                   1,14,14,20,8,1,4,1,5,2,2,2,1,1,10,1,3,
                   17,14,2,5,12,6,30,30,12,6,5,1,1,2,3,14,
                   14,1,1,14,6,6,12,20,20,2,3,2,1,2,1,10,
                   1,1,1,10,10,10,10,10,6,10,10,10,10,3,
                   2,10,3,3,1,1,1,1,2,6,1,1,2,2,10,10,10,
                   10,10,10,1,1,10,1,1,10,10,2,2,12,10,1,
                   4,8,1,6,8,4,4,4,4,8,4,4,4,20,1,6,5,8,
                   1,2,4,4,10,1,2,1,2,4,10,1,1,1,1,1,36,
                   36,3,14,1,2,4,10,1,1,2,4,10,1,1,2,4,
                   10,1,13,2)
    parse = make_parser(fieldwidths)

    # Create Contained for all Master Records
    masterList = []

    # Go Through All Lines in Document
    j = 0
    lines = f.readlines()
    for line in lines:
        fields = parse(line)

        # Assign every pasrsed field to corresponding dict descriptor
        d = OrderedDict()
        d['CM_CUSTID']=fields[0].rstrip().lstrip()
        d['CM_NAME']=fields[1].rstrip().lstrip()
        d['CM_ADDR1']=fields[2].rstrip().lstrip()
        d['CM_ADDR2']=fields[3].rstrip().lstrip()
        d['CM_CONTACT']=fields[4].rstrip().lstrip()
        d['CM_PHONE']=fields[5].rstrip().lstrip()
        d['CM_TOUCODE']=fields[6].rstrip().lstrip()
        d['CM_SIC']=fields[7].rstrip().lstrip()
        d['CM_ACCOUNT']=fields[8].rstrip().lstrip()
        d['CM_CYCLE']=fields[9].rstrip().lstrip()
        d['CM_CPAD2']=fields[10].rstrip().lstrip()
        d['CM_MULREC']=fields[11].rstrip().lstrip()
        d['CM_STRATA']=fields[12].rstrip().lstrip()
        d['CM_UPLDATE']=fields[13].rstrip().lstrip()
        d['CM_LOGCHANS']=fields[14].rstrip().lstrip()
        d['CM_MASTSCHED']=fields[15].rstrip().lstrip()
        d['CM_APPLCODE']=fields[16].rstrip().lstrip()
        d['CM_GROUP']=fields[17].rstrip().lstrip()
        d['CM_AUTOUPLD']=fields[18].rstrip().lstrip()
        d['CM_RATE']=fields[19].rstrip().lstrip()
        d['CM_MFPEND']=fields[20].rstrip().lstrip()
        d['CM_CHDATE']=fields[21].rstrip().lstrip()
        d['CM_CHTYPE']=fields[22].rstrip().lstrip()
        d['CM_COMMENT']=fields[23].rstrip().lstrip()
        d['CM_PRCUSTID']=fields[24].rstrip().lstrip()
        d['CM_MASTMTRID']=fields[25].rstrip().lstrip()
        d['CM_CPAD1']=fields[26].rstrip().lstrip()
        d['CM_ENDTIME']=fields[27].rstrip().lstrip()
        d['CM_RPTSTART']=fields[28].rstrip().lstrip()
        d['CM_RPTSTOP']=fields[29].rstrip().lstrip()
        d['CM_RPTFILE']=fields[30].rstrip().lstrip()
        d['CM_DL1']=fields[31].rstrip().lstrip()
        d['CM_DL2']=fields[32].rstrip().lstrip()
        d['CM_PF1']=fields[33].rstrip().lstrip()
        d['CM_PF2']=fields[34].rstrip().lstrip()
        d['CM_PFDL']=fields[35].rstrip().lstrip()
        d['CM_BILLKVAR']=fields[36].rstrip().lstrip()
        d['CM_LCTYPE']=fields[37].rstrip().lstrip()
        d['CM_FSLGLD']=fields[38].rstrip().lstrip()
        d['CM_OPTDATA']=fields[39].rstrip().lstrip()
        d['CM_CRITPROG']=fields[40].rstrip().lstrip()
        d['PAD1']=fields[41].rstrip().lstrip()
        d['RM_RECID']=fields[42].rstrip().lstrip()
        d['RM_GROUP']=fields[43].rstrip().lstrip()
        d['RM_LOCAT']=fields[44].rstrip().lstrip()
        d['RM_INPHR']=fields[45].rstrip().lstrip()
        d['RM_CHAN']=fields[46].rstrip().lstrip()
        d['RM_DEVTYP']=fields[47].rstrip().lstrip()
        d['RM_RIPEND']=fields[48].rstrip().lstrip()
        d['RM_TIM']=fields[49].rstrip().lstrip()
        d['RM_NRINGS']=fields[50].rstrip().lstrip()
        d['RM_CALLMODE']=fields[51].rstrip().lstrip()
        d['RM_LPCPHONE']=fields[52].rstrip().lstrip()
        d['RM_RPCPHONE']=fields[53].rstrip().lstrip()
        d['RM_BAUD']=fields[54].rstrip().lstrip()
        d['RM_TIMESET']=fields[55].rstrip().lstrip()
        d['RM_AWDAYS']=fields[56].rstrip().lstrip()
        d['RM_LMTCALL']=fields[57].rstrip().lstrip()
        d['RM_UADDR']=fields[58].rstrip().lstrip()
        d['RM_TZONEADJ']=fields[59].rstrip().lstrip()
        d['RM_ANSWINDOW']=fields[60].rstrip().lstrip()
        d['RM_METERCGF']=fields[61].rstrip().lstrip()
        d['RM_PW1']=fields[62].rstrip().lstrip()
        d['RM_PW2']=fields[63].rstrip().lstrip()
        d['RM_STATUSINPUTS']=fields[64].rstrip().lstrip()
        d['RM_BAUD1']=fields[65].rstrip().lstrip()
        d['RM_VOLTAGE']=fields[66].rstrip().lstrip()
        d['RM_METLOC']=fields[67].rstrip().lstrip()
        d['RM_READINS']=fields[68].rstrip().lstrip()
        d['RM_CHOFFSET']=fields[69].rstrip().lstrip()
        d['PAD2']=fields[70].rstrip().lstrip()
        d['RM_INITDATE']=fields[71].rstrip().lstrip()
        d['RM_LOCALPW']=fields[72].rstrip().lstrip()
        d['RM_BATLOGD']=fields[73].rstrip().lstrip()
        d['RM_BATLOGM']=fields[74].rstrip().lstrip()
        d['RM_LASTTIME']=fields[75].rstrip().lstrip()
        d['RM_MEMSIZE']=fields[76].rstrip().lstrip()
        d['RM_DCHAIN']=fields[77].rstrip().lstrip()
        d['RM_DCMASTER']=fields[78].rstrip().lstrip()
        d['RM_DCSLAVE']=fields[79].rstrip().lstrip()
        d['RM_DEVID']=fields[80].rstrip().lstrip()
        d['RM_DEVSN']=fields[81].rstrip().lstrip()
        d['RM_LOP']=fields[82].rstrip().lstrip()
        d['RM_CALLTIME']=fields[83].rstrip().lstrip()
        d['RM_METERCHG']=fields[84].rstrip().lstrip()
        d['RM_RELAYTYPE']=fields[85].rstrip().lstrip()
        d['RM_STATUS']=fields[86].rstrip().lstrip()
        d['RM_RPC']=fields[87].rstrip().lstrip()
        d['RM_TOTMETERS']=fields[88].rstrip().lstrip()
        d['RM_DST']=fields[89].rstrip().lstrip()
        d['RM_FWDCALL']=fields[90].rstrip().lstrip()
        d['RM_CHDATE']=fields[91].rstrip().lstrip()
        d['RM_CHTYPE']=fields[92].rstrip().lstrip()
        d['RM_TIMETOL']=fields[93].rstrip().lstrip()
        d['PAD3']=fields[94].rstrip().lstrip()
        d['RM_RDRECID']=fields[95].rstrip().lstrip()
        d['RM_PAD3']=fields[96].rstrip().lstrip()
        d['RM_ALARM']=fields[97].rstrip().lstrip()
        d['RM_MFILE']=fields[98].rstrip().lstrip()
        d['RM_BATINSTD']=fields[99].rstrip().lstrip()
        d['RM_INPUTDESC']=fields[100].rstrip().lstrip()
        d['RM_RELAYDESC']=fields[101].rstrip().lstrip()
        d['RM_CFGNAME']=fields[102].rstrip().lstrip()
        d['RM_ROUTE']=fields[103].rstrip().lstrip()
        d['RM_SEQROUTE']=fields[104].rstrip().lstrip()
        d['RM_PLUG_OPT']=fields[105].rstrip().lstrip()
        d['RM_PLUG_OUT']=fields[106].rstrip().lstrip()
        d['RM_PLUG_MISS']=fields[107].rstrip().lstrip()
        d['RM_PLUG_PER']=fields[108].rstrip().lstrip()
        d['RM_PLUG_REF']=fields[109].rstrip().lstrip()
        d['RM_CHROM_ID']=fields[110].rstrip().lstrip()
        d['MM_UOMSCALE']=fields[111].rstrip().lstrip()
        d['RM_OLDDST']=fields[112].rstrip().lstrip()
        d['MM_RECID']=fields[113].rstrip().lstrip()
        d['MM_PAD1']=fields[114].rstrip().lstrip()
        d['MM_GROUP']=fields[115].rstrip().lstrip()
        d['MM_METERID']=fields[116].rstrip().lstrip()
        d['MM_CUSTID']=fields[117].rstrip().lstrip()
        d['MM_XFACCT']=fields[118].rstrip().lstrip()
        d['MM_PYSCHAN']=fields[119].rstrip().lstrip()
        d['MM_LOGCHAN']=fields[120].rstrip().lstrip()
        d['MM_UMCODE']=fields[121].rstrip().lstrip()
        d['MM_REGTYPE']=fields[122].rstrip().lstrip()
        d['MM_ENCTYPE']=fields[123].rstrip().lstrip()
        d['MM_NRKVARH']=fields[124].rstrip().lstrip()
        d['MM_ENCBASE']=fields[125].rstrip().lstrip()
        d['MM_NDIALS']=fields[126].rstrip().lstrip()
        d['MM_MREADS']=fields[127].rstrip().lstrip()
        d['MM_CFORM']=fields[128].rstrip().lstrip()
        d['MM_LASTENC']=fields[129].rstrip().lstrip()
        d['MM_LASTVIS']=fields[130].rstrip().lstrip()
        d['MM_MMULT']=fields[131].rstrip().lstrip()
        d['MM_PMULT']=fields[132].rstrip().lstrip()
        d['MM_POFFS']=fields[133].rstrip().lstrip()
        d['MM_TOLPCT']=fields[134].rstrip().lstrip()
        d['MM_MAXINT']=fields[135].rstrip().lstrip()
        d['MM_MININT']=fields[136].rstrip().lstrip()
        d['MM_MAXTOT']=fields[137].rstrip().lstrip()
        d['MM_MINTOT']=fields[138].rstrip().lstrip()
        d['MM_PCTCHG']=fields[139].rstrip().lstrip()
        d['MM_CFCODE']=fields[140].rstrip().lstrip()
        d['MM_LASTKVR']=fields[141].rstrip().lstrip()
        d['MM_LFTOL']=fields[142].rstrip().lstrip()
        d['MM_PFTOL']=fields[143].rstrip().lstrip()
        d['MM_KVARH']=fields[144].rstrip().lstrip()
        d['MM_TOLTYPE']=fields[145].rstrip().lstrip()
        d['MM_DECPOS']=fields[146].rstrip().lstrip()
        d['MM_PAD2']=fields[147].rstrip().lstrip()
        d['MM_RDCHAN']=fields[148].rstrip().lstrip()
        d['MM_PTRATIO']=fields[149].rstrip().lstrip()
        d['MM_SERVTYPE']=fields[150].rstrip().lstrip()
        d['MM_OMITUPLD']=fields[151].rstrip().lstrip()
        d['MM_KVASET']=fields[152].rstrip().lstrip()
        d['MM_LOSSOPT']=fields[153].rstrip().lstrip()
        d['MM_WILC']=fields[154].rstrip().lstrip()
        d['MM_WCLC']=fields[155].rstrip().lstrip()
        d['MM_VILC']=fields[156].rstrip().lstrip()
        d['MM_VCLC']=fields[157].rstrip().lstrip()
        d['MM_VOLTS']=fields[158].rstrip().lstrip()
        d['MM_RDTOL']=fields[159].rstrip().lstrip()
        d['MM_PHASE']=fields[160].rstrip().lstrip()
        d['MM_FLOW']=fields[161].rstrip().lstrip()
        d['MM_CHDATE']=fields[162].rstrip().lstrip()
        d['MM_CHTYPE']=fields[163].rstrip().lstrip()
        d['MM_OPTDATA']=fields[164].rstrip().lstrip()
        d['MM_TOTPULSE']=fields[165].rstrip().lstrip()
        d['MM_METINSTD']=fields[166].rstrip().lstrip()
        d['MM_MAPCHAN']=fields[167].rstrip().lstrip()
        d['MM_METSEQ']=fields[168].rstrip().lstrip()
        d['MM_NRKVSN']=fields[169].rstrip().lstrip()
        d['MM_NRKVMULT']=fields[170].rstrip().lstrip()
        d['MM_NRKDIALS']=fields[171].rstrip().lstrip()
        d['MM_ZEROINT']=fields[172].rstrip().lstrip()
        d['MM_CORRFACT']=fields[173].rstrip().lstrip()
        d['RM_ MTRTYPE']=fields[174].rstrip().lstrip()
        d['MM_ABSDIFF']=fields[175].rstrip().lstrip()
        d['RM_CONNTYPE']=fields[176].rstrip().lstrip()
        d['RM_ INBOUNDCALLDELAY1']=fields[177].rstrip().lstrip()
        d['RM_ INBOUNDCALLDELAY2']=fields[178].rstrip().lstrip()
        d['RM_NUMCALLS']=fields[179].rstrip().lstrip()
        d['RM_NUMTRIES']=fields[180].rstrip().lstrip()
        d['RM_INSCID']=fields[181].rstrip().lstrip()
        d['RM_CURINTVLEN']=fields[182].rstrip().lstrip()
        d['RM_UPDINTVLEN']=fields[183].rstrip().lstrip()
        d['RM_SLOTNUMBER']=fields[184].rstrip().lstrip()
        d['RM_ SERVICE_BASED_ID']=fields[185].rstrip().lstrip()
        d['RM_EXPANDED']=fields[186].rstrip().lstrip()
        d['RM_IP_PORT']=fields[187].rstrip().lstrip()
        d['PAD4']=fields[188].rstrip().lstrip()
        d['CM_BBSLIB']=fields[189].rstrip().lstrip()
        d['CM_PBS']=fields[190].rstrip().lstrip()
        d['CM_RPCYCLE']=fields[191].rstrip().lstrip()
        d['CM_RPENDTIME']=fields[192].rstrip().lstrip()
        d['CM_A1ENDTIME']=fields[193].rstrip().lstrip()
        d['CM_A1UPLDATE']=fields[194].rstrip().lstrip()
        d['CM_A1AUTOPLD']=fields[195].rstrip().lstrip()
        d['CM_A1CYCLE']=fields[196].rstrip().lstrip()
        d['CM_A2AUTOUPLD']=fields[197].rstrip().lstrip()
        d['CM_A2CYCLE']=fields[198].rstrip().lstrip()
        d['CM_A2ENDTIME']=fields[199].rstrip().lstrip()
        d['CM_A2UPLDATE']=fields[200].rstrip().lstrip()
        d['CM_A1MFPEND']=fields[201].rstrip().lstrip()
        d['CM_A2MFPEND']=fields[202].rstrip().lstrip()
        d['N/A']=fields[203].rstrip().lstrip()
        d['N/A']=fields[204].rstrip().lstrip()
        d['N/A']=fields[205].rstrip().lstrip()
        d['RM_HOMEPHONE1']=fields[206].rstrip().lstrip()
        d['RM_HOMEPHONE2']=fields[207].rstrip().lstrip()
        d['RM_ CORRECTIONFACTORTOL']=fields[208].rstrip().lstrip()
        d['PAD5']=fields[209].rstrip().lstrip()
        d['CM_A3AUTOUPLD']=fields[210].rstrip().lstrip()
        d['CM_A3CYCLE']=fields[211].rstrip().lstrip()
        d['CM_A3ENDTIME']=fields[212].rstrip().lstrip()
        d['CM_A3UPLDATE']=fields[213].rstrip().lstrip()
        d['CM_A3MFPEND']=fields[214].rstrip().lstrip()
        d['CM_A4AUTOUPLD']=fields[215].rstrip().lstrip()
        d['CM_A4CYCLE']=fields[216].rstrip().lstrip()
        d['CM_A4ENDTIME']=fields[217].rstrip().lstrip()
        d['CM_A4UPLDATE']=fields[218].rstrip().lstrip()
        d['CM_A4MFPEND']=fields[219].rstrip().lstrip()
        d['CM_A5AUTOPLD']=fields[220].rstrip().lstrip()
        d['CM_A5CYCLE']=fields[221].rstrip().lstrip()
        d['CM_A5ENDTIME']=fields[222].rstrip().lstrip()
        d['CM_A5UPLDATE']=fields[223].rstrip().lstrip()
        d['CM_A5MFPEND']=fields[224].rstrip().lstrip()
        d['PAD6']=fields[225].rstrip().lstrip()
        d['PAD7']=fields[226].rstrip().lstrip()

        # Print the Channel being printed
        print('Logical Channel: '+d['MM_LOGCHAN'])
        for k, v in d.items():
            print( k+'|'+v)
        print('\n')


    
    # Close the Master File
    f.close()



    
