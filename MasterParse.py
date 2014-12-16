import csv
import sys
from operator import itemgetter, attrgetter
import os
import itertools

from sqlalchemy_declarative import Customer, Channel, Recorder, Base

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

        # Create Objects to Contain MF Records
        dCM = Customer(
            CM_A1AUTOPLD=stripStr(fields[186]), 
            CM_A1CYCLE=stripStr(fields[187]), 
            CM_A1ENDTIME=stripStr(fields[184]), 
            CM_A1MFPEND=stripStr(fields[192]), 
            CM_A1UPLDATE=stripStr(fields[185]), 
            CM_A2AUTOUPLD=stripStr(fields[188]), 
            CM_A2CYCLE=stripStr(fields[189]), 
            CM_A2ENDTIME=stripStr(fields[190]), 
            CM_A2MFPEND=stripStr(fields[193]), 
            CM_A2UPLDATE=stripStr(fields[191]), 
            CM_A3AUTOUPLD=stripStr(fields[200]), 
            CM_A3CYCLE=stripStr(fields[201]), 
            CM_A3ENDTIME=stripStr(fields[202]), 
            CM_A3MFPEND=stripStr(fields[204]), 
            CM_A3UPLDATE=stripStr(fields[203]), 
            CM_A4AUTOUPLD=stripStr(fields[205]), 
            CM_A4CYCLE=stripStr(fields[206]), 
            CM_A4ENDTIME=stripStr(fields[207]), 
            CM_A4MFPEND=stripStr(fields[209]), 
            CM_A4UPLDATE=stripStr(fields[208]), 
            CM_A5AUTOPLD=stripStr(fields[210]), 
            CM_A5CYCLE=stripStr(fields[211]), 
            CM_A5ENDTIME=stripStr(fields[212]), 
            CM_A5MFPEND=stripStr(fields[214]), 
            CM_A5UPLDATE=stripStr(fields[213]), 
            CM_ACCOUNT=stripStr(fields[8]), 
            CM_ADDR1=stripStr(fields[2]), 
            CM_ADDR2=stripStr(fields[3]), 
            CM_APPLCODE=stripStr(fields[15]), 
            CM_AUTOUPLD=stripStr(fields[17]), 
            CM_BBSLIB=stripStr(fields[180]), 
            CM_BILLKVAR=stripStr(fields[34]), 
            CM_CHDATE=stripStr(fields[20]), 
            CM_CHTYPE=stripStr(fields[21]), 
            CM_COMMENT=stripStr(fields[22]), 
            CM_CONTACT=stripStr(fields[4]), 
            CM_CRITPROG=stripStr(fields[38]), 
            CM_CUSTID=stripStr(fields[0]), 
            CM_CYCLE=stripStr(fields[9]), 
            CM_DL1=stripStr(fields[29]), 
            CM_DL2=stripStr(fields[30]), 
            CM_ENDTIME=stripStr(fields[25]), 
            CM_FSLGLD=stripStr(fields[36]), 
            CM_GROUP=stripStr(fields[16]), 
            CM_LCTYPE=stripStr(fields[35]), 
            CM_LOGCHANS=stripStr(fields[13]), 
            CM_MASTMTRID=stripStr(fields[24]), 
            CM_MASTSCHED=stripStr(fields[14]), 
            CM_MFPEND=stripStr(fields[19]), 
            CM_MULREC=stripStr(fields[10]), 
            CM_NAME=stripStr(fields[1]), 
            CM_OPTDATA=stripStr(fields[37]), 
            CM_PBS=stripStr(fields[181]), 
            CM_PF1=stripStr(fields[31]), 
            CM_PF2=stripStr(fields[32]), 
            CM_PFDL=stripStr(fields[33]), 
            CM_PHONE=stripStr(fields[5]), 
            CM_PRCUSTID=stripStr(fields[23]), 
            CM_RATE=stripStr(fields[18]), 
            CM_RPCYCLE=stripStr(fields[182]), 
            CM_RPENDTIME=stripStr(fields[183]), 
            CM_RPTFILE=stripStr(fields[28]), 
            CM_RPTSTART=stripStr(fields[26]), 
            CM_RPTSTOP=stripStr(fields[27]), 
            CM_SIC=stripStr(fields[7]), 
            CM_STRATA=stripStr(fields[11]), 
            CM_TOUCODE=stripStr(fields[6]), 
            CM_UPLDATE=stripStr(fields[12])
        )

        dMM = Channel(
            MM_ABSDIFF=stripStr(fields[167]), 
            MM_CFCODE=stripStr(fields[133]), 
            MM_CFORM=stripStr(fields[121]), 
            MM_CHDATE=stripStr(fields[154]), 
            MM_CHTYPE=stripStr(fields[155]), 
            MM_CORRFACT=stripStr(fields[165]), 
            MM_CUSTID=stripStr(fields[110]), 
            MM_DECPOS=stripStr(fields[139]), 
            MM_ENCBASE=stripStr(fields[118]), 
            MM_ENCTYPE=stripStr(fields[116]), 
            MM_Extra1=stripStr(fields[194]), 
            MM_Extra2=stripStr(fields[195]), 
            MM_Extra3=stripStr(fields[196]), 
            MM_FLOW=stripStr(fields[153]), 
            MM_GROUP=stripStr(fields[108]), 
            MM_KVARH=stripStr(fields[137]), 
            MM_KVASET=stripStr(fields[144]), 
            MM_LASTENC=stripStr(fields[122]), 
            MM_LASTKVR=stripStr(fields[134]), 
            MM_LASTVIS=stripStr(fields[123]), 
            MM_LFTOL=stripStr(fields[135]), 
            MM_LOGCHAN=stripStr(fields[113]), 
            MM_LOSSOPT=stripStr(fields[145]), 
            MM_MAPCHAN=stripStr(fields[159]), 
            MM_MAXINT=stripStr(fields[128]), 
            MM_MAXTOT=stripStr(fields[130]), 
            MM_METERID=stripStr(fields[109]), 
            MM_METINSTD=stripStr(fields[158]), 
            MM_METSEQ=stripStr(fields[160]), 
            MM_MININT=stripStr(fields[129]), 
            MM_MINTOT=stripStr(fields[131]), 
            MM_MMULT=stripStr(fields[124]), 
            MM_MREADS=stripStr(fields[120]), 
            MM_NDIALS=stripStr(fields[119]), 
            MM_NRKDIALS=stripStr(fields[163]), 
            MM_NRKVARH=stripStr(fields[117]), 
            MM_NRKVMULT=stripStr(fields[162]), 
            MM_NRKVSN=stripStr(fields[161]), 
            MM_OMITUPLD=stripStr(fields[143]), 
            MM_OPTDATA=stripStr(fields[156]), 
            MM_PCTCHG=stripStr(fields[132]), 
            MM_PFTOL=stripStr(fields[136]), 
            MM_PHASE=stripStr(fields[152]), 
            MM_PMULT=stripStr(fields[125]), 
            MM_POFFS=stripStr(fields[126]), 
            MM_PTRATIO=stripStr(fields[141]), 
            MM_PYSCHAN=stripStr(fields[112]), 
            MM_RDCHAN=stripStr(fields[140]), 
            MM_RDTOL=stripStr(fields[151]), 
            MM_RECID=stripStr(fields[107]), 
            MM_REGTYPE=stripStr(fields[115]), 
            MM_SERVTYPE=stripStr(fields[142]), 
            MM_TOLPCT=stripStr(fields[127]), 
            MM_TOLTYPE=stripStr(fields[138]), 
            MM_TOTPULSE=stripStr(fields[157]), 
            MM_UMCODE=stripStr(fields[114]), 
            MM_UOMSCALE=stripStr(fields[105]), 
            MM_VCLC=stripStr(fields[149]), 
            MM_VILC=stripStr(fields[148]), 
            MM_VOLTS=stripStr(fields[150]), 
            MM_WCLC=stripStr(fields[147]), 
            MM_WILC=stripStr(fields[146]), 
            MM_XFACCT=stripStr(fields[111]), 
            MM_ZEROINT=stripStr(fields[164])
        )

        dRM = Recorder(
            RM_CORRECTIONFACTORTOL=stripStr(fields[199]), 
            RM_INBOUNDCALLDELAY1=stripStr(fields[169]), 
            RM_INBOUNDCALLDELAY2=stripStr(fields[170]), 
            RM_MTRTYPE=stripStr(fields[166]), 
            RM_SERVICE_BASED_ID=stripStr(fields[177]), 
            RM_ALARM=stripStr(fields[91]), 
            RM_ANSWINDOW=stripStr(fields[57]), 
            RM_AWDAYS=stripStr(fields[53]), 
            RM_BATINSTD=stripStr(fields[93]), 
            RM_BATLOGD=stripStr(fields[69]), 
            RM_BATLOGM=stripStr(fields[70]), 
            RM_BAUD=stripStr(fields[51]), 
            RM_BAUD1=stripStr(fields[62]), 
            RM_CALLMODE=stripStr(fields[48]), 
            RM_CALLTIME=stripStr(fields[79]), 
            RM_CFGNAME=stripStr(fields[96]), 
            RM_CHAN=stripStr(fields[43]), 
            RM_CHDATE=stripStr(fields[87]), 
            RM_CHOFFSET=stripStr(fields[66]), 
            RM_CHROM_ID=stripStr(fields[104]), 
            RM_CHTYPE=stripStr(fields[88]), 
            RM_CONNTYPE=stripStr(fields[168]), 
            RM_CURINTVLEN=stripStr(fields[174]), 
            RM_DCHAIN=stripStr(fields[73]), 
            RM_DCMASTER=stripStr(fields[74]), 
            RM_DCSLAVE=stripStr(fields[75]), 
            RM_DEVID=stripStr(fields[76]), 
            RM_DEVSN=stripStr(fields[77]), 
            RM_DEVTYP=stripStr(fields[44]), 
            RM_DST=stripStr(fields[85]), 
            RM_EXPANDED=stripStr(fields[178]), 
            RM_FWDCALL=stripStr(fields[86]), 
            RM_GROUP=stripStr(fields[40]), 
            RM_HOMEPHONE1=stripStr(fields[197]), 
            RM_HOMEPHONE2=stripStr(fields[198]), 
            RM_INITDATE=stripStr(fields[67]), 
            RM_INPHR=stripStr(fields[42]), 
            RM_INPUTDESC=stripStr(fields[94]), 
            RM_INSCID=stripStr(fields[173]), 
            RM_IP_PORT=stripStr(fields[179]), 
            RM_LASTTIME=stripStr(fields[71]), 
            RM_LMTCALL=stripStr(fields[54]), 
            RM_LOCALPW=stripStr(fields[68]), 
            RM_LOCAT=stripStr(fields[41]), 
            RM_LOP=stripStr(fields[78]), 
            RM_LPCPHONE=stripStr(fields[49]), 
            RM_MEMSIZE=stripStr(fields[72]), 
            RM_METERCGF=stripStr(fields[58]), 
            RM_METERCHG=stripStr(fields[80]), 
            RM_METLOC=stripStr(fields[64]), 
            RM_MFILE=stripStr(fields[92]), 
            RM_NRINGS=stripStr(fields[47]), 
            RM_NUMCALLS=stripStr(fields[171]), 
            RM_NUMTRIES=stripStr(fields[172]), 
            RM_OLDDST=stripStr(fields[106]), 
            RM_PLUG_MISS=stripStr(fields[101]), 
            RM_PLUG_OPT=stripStr(fields[99]), 
            RM_PLUG_OUT=stripStr(fields[100]), 
            RM_PLUG_PER=stripStr(fields[102]), 
            RM_PLUG_REF=stripStr(fields[103]), 
            RM_PW1=stripStr(fields[59]), 
            RM_PW2=stripStr(fields[60]), 
            RM_RDRECID=stripStr(fields[90]), 
            RM_READINS=stripStr(fields[65]), 
            RM_RECID=stripStr(fields[39]), 
            RM_RELAYDESC=stripStr(fields[95]), 
            RM_RELAYTYPE=stripStr(fields[81]), 
            RM_RIPEND=stripStr(fields[45]), 
            RM_ROUTE=stripStr(fields[97]), 
            RM_RPC=stripStr(fields[83]), 
            RM_RPCPHONE=stripStr(fields[50]), 
            RM_SEQROUTE=stripStr(fields[98]), 
            RM_SLOTNUMBER=stripStr(fields[176]), 
            RM_STATUS=stripStr(fields[82]), 
            RM_STATUSINPUTS=stripStr(fields[61]), 
            RM_TIM=stripStr(fields[46]), 
            RM_TIMESET=stripStr(fields[52]), 
            RM_TIMETOL=stripStr(fields[89]), 
            RM_TOTMETERS=stripStr(fields[84]), 
            RM_TZONEADJ=stripStr(fields[56]), 
            RM_UADDR=stripStr(fields[55]), 
            RM_UPDINTVLEN=stripStr(fields[175]), 
            RM_VOLTAGE=stripStr(fields[63])
        )

        # Add Dicts to List of Dicts
        recorderList.append(dRM)
        customerList.append(dCM)
        channelList.append(dMM)

    # Add recorders, customers, and channels to master return; make recorder and customer deduped
    masterList.append(set(recorderList))
    masterList.append(set(customerList))
    masterList.append(channelList)
    
    # Close the Master File
    f.close()

    return(masterList)

# Standalone Script Section
if __name__ == "__main__":
    fname = r'C:\Users\CJANCSAR\Documents\FNBUG\ALL.DAT'
    ret = parseMaster(fname)

    for mem in ret[1]:
        print(mem.CM_CUSTID)
 
