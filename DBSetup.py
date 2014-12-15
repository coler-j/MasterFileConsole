###############################################################################
# This is strictly to establish a SQLITE DB for demo or standalone operation
#
# Author: Cole Jancsar
# Created: 12/13/2014
#
###############################################################################
import sqlite3

###############################################################################
# Utility Functions
###############################################################################

# Function to check if table exists in db
# Input: String Containing tablename
# Output: True for table exists; False for table not exists
def tableExist(tableName):
    # Check to See if table exists
    cursor.execute('''
        SELECT name FROM sqlite_master WHERE type='table' AND name=?
    ''', (tableName,))

    # Get return value
    ret = cursor.fetchone()

    if ret is not None:
        return True
    else:
        return False



###############################################################################
# Main Logic
###############################################################################

# Main Body of Setup Script
if __name__ == "__main__":
    # Open or Create a SQLite DB
    db = sqlite3.connect(r'C:\Users\CJANCSAR\Documents\FNBUG\Data\MV90ExtensionDB')

    # Get a cursor object to DB
    cursor = db.cursor()

    #######################
    # Create Various Tables
    #######################
    # MF_Customer table for Customer portion of MF
    if not tableExist('MF_Customer'):
        print('Creating MF_Customer table')
        cursor.execute('''
            CREATE TABLE MF_Customer(
                CM_MASTMTRID TEXT,
                CM_CUSTID TEXT,
                CM_A1AUTOPLD TEXT,
                CM_A1CYCLE TEXT,
                CM_A1ENDTIME INTEGER,
                CM_A1MFPEND TEXT,
                CM_A1UPLDATE INTEGER,
                CM_A2AUTOUPLD TEXT,
                CM_A2CYCLE TEXT,
                CM_A2ENDTIME INTEGER,
                CM_A2MFPEND TEXT,
                CM_A2UPLDATE INTEGER,
                CM_A3AUTOUPLD TEXT,
                CM_A3CYCLE TEXT,
                CM_A3ENDTIME INTEGER,
                CM_A3MFPEND TEXT,
                CM_A3UPLDATE INTEGER,
                CM_A4AUTOUPLD TEXT,
                CM_A4CYCLE TEXT,
                CM_A4ENDTIME INTEGER,
                CM_A4MFPEND TEXT,
                CM_A4UPLDATE INTEGER,
                CM_A5AUTOPLD TEXT,
                CM_A5CYCLE TEXT,
                CM_A5ENDTIME INTEGER,
                CM_A5MFPEND TEXT,
                CM_A5UPLDATE INTEGER,
                CM_ACCOUNT TEXT,
                CM_ADDR1 TEXT,
                CM_ADDR2 TEXT,
                CM_APPLCODE TEXT,
                CM_AUTOUPLD TEXT,
                CM_BBSLIB TEXT,
                CM_BILLKVAR TEXT,
                CM_CHDATE INTEGER,
                CM_CHTYPE TEXT,
                CM_COMMENT TEXT,
                CM_CONTACT TEXT,
                CM_CRITPROG TEXT,
                CM_CYCLE TEXT,
                CM_DL1 REAL,
                CM_DL2 REAL,
                CM_ENDTIME INTEGER,
                CM_FSLGLD TEXT,
                CM_GROUP TEXT,
                CM_LCTYPE TEXT,
                CM_LOGCHANS INT,
                CM_MASTSCHED TEXT,
                CM_MFPEND TEXT,
                CM_MULREC TEXT,
                CM_NAME TEXT,
                CM_OPTDATA TEXT,
                CM_PBS TEXT,
                CM_PF1 REAL,
                CM_PF2 REAL,
                CM_PFDL REAL,
                CM_PHONE TEXT,
                CM_PRCUSTID TEXT,
                CM_RATE TEXT,
                CM_RPCYCLE TEXT,
                CM_RPENDTIME INTEGER,
                CM_RPTFILE TEXT,
                CM_RPTSTART INTEGER,
                CM_RPTSTOP INTEGER,
                CM_SIC TEXT,
                CM_STRATA TEXT,
                CM_TOUCODE TEXT,
                CM_UPLDATE INTEGER
            )
         ''')
        db.commit()
    else:
        print('MF_Customer table already exists')

    # MF_Recorder table for Recorder portion of MF
    if not tableExist('MF_Recorder'):
        print('Creating MF_Recorder table')
        cursor.execute('''
            CREATE TABLE MF_Recorder(
                RM_CORRECTIONFACTORTOL TEXT,
                RM_INBOUNDCALLDELAY1 TEXT,
                RM_INBOUNDCALLDELAY2 TEXT,
                RM_MTRTYPE TEXT,
                RM_SERVICE_BASED_ID TEXT,
                RM_ALARM TEXT,
                RM_ANSWINDOW INTEGER,
                RM_AWDAYS TEXT,
                RM_BATINSTD INTEGER,
                RM_BATLOGD INTEGER,
                RM_BATLOGM INTEGER,
                RM_BAUD INTEGER,
                RM_BAUD1 TEXT,
                RM_CALLMODE TEXT,
                RM_CALLTIME INTEGER,
                RM_CFGNAME TEXT,
                RM_CHAN INTEGER,
                RM_CHDATE INTEGER,
                RM_CHOFFSET TEXT,
                RM_CHROM_ID TEXT,
                RM_CHTYPE TEXT,
                RM_CONNTYPE TEXT,
                RM_CURINTVLEN INTEGER,
                RM_DCHAIN TEXT,
                RM_DCMASTER TEXT,
                RM_DCSLAVE TEXT,
                RM_DEVID TEXT,
                RM_DEVSN TEXT,
                RM_DEVTYP TEXT,
                RM_DST TEXT,
                RM_EXPANDED TEXT,
                RM_FWDCALL TEXT,
                RM_GROUP TEXT,
                RM_HOMEPHONE1 TEXT,
                RM_HOMEPHONE2 TEXT,
                RM_INITDATE INTEGER,
                RM_INPHR INTEGER,
                RM_INPUTDESC TEXT,
                RM_INSCID TEXT,
                RM_IP_PORT TEXT,
                RM_LASTTIME INTEGER,
                RM_LMTCALL INTEGER,
                RM_LOCALPW TEXT,
                RM_LOCAT TEXT,
                RM_LOP TEXT,
                RM_LPCPHONE TEXT,
                RM_MEMSIZE INTEGER,
                RM_METERCGF TEXT,
                RM_METERCHG TEXT,
                RM_METLOC TEXT,
                RM_MFILE TEXT,
                RM_NRINGS INTEGER,
                RM_NUMCALLS INTEGER,
                RM_NUMTRIES INTEGER,
                RM_OLDDST TEXT,
                RM_PLUG_MISS TEXT,
                RM_PLUG_OPT TEXT,
                RM_PLUG_OUT TEXT,
                RM_PLUG_PER TEXT,
                RM_PLUG_REF TEXT,
                RM_PW1 TEXT,
                RM_PW2 TEXT,
                RM_RDRECID TEXT,
                RM_READINS TEXT,
                RM_RECID TEXT,
                RM_RELAYDESC TEXT,
                RM_RELAYTYPE TEXT,
                RM_RIPEND TEXT,
                RM_ROUTE TEXT,
                RM_RPC TEXT,
                RM_RPCPHONE TEXT,
                RM_SEQROUTE TEXT,
                RM_SLOTNUMBER TEXT,
                RM_STATUS TEXT,
                RM_STATUSINPUTS TEXT,
                RM_TIM INTEGER,
                RM_TIMESET TEXT,
                RM_TIMETOL TEXT,
                RM_TOTMETERS INTEGER,
                RM_TZONEADJ INTEGER,
                RM_UADDR INTEGER,
                RM_UPDINTVLEN INTEGER,
                RM_VOLTAGE TEXT
            )
         ''')
        db.commit()
    else:
        print('MF_Recorder table already exists')
        
    # MF_Channel table for Channel portion of MF
    if not tableExist('MF_Channel'):
        print('Creating MF_Channel table')
        cursor.execute('''
            CREATE TABLE MF_Channel(
                MM_ABSDIFF REAL,
                MM_CFCODE REAL,
                MM_CFORM TEXT,
                MM_CHDATE INTEGER,
                MM_CHTYPE TEXT,
                MM_CORRFACT REAL,
                MM_CUSTID TEXT,
                MM_DECPOS INTEGER,
                MM_ENCBASE REAL,
                MM_ENCTYPE TEXT,
                MM_FLOW TEXT,
                MM_GROUP TEXT,
                MM_KVARH TEXT,
                MM_KVASET INTEGER,
                MM_LASTENC REAL,
                MM_LASTKVR TEXT,
                MM_LASTVIS REAL,
                MM_LFTOL REAL,
                MM_LOGCHAN INTEGER,
                MM_LOSSOPT TEXT,
                MM_MAPCHAN TEXT,
                MM_MAXINT REAL,
                MM_MAXTOT REAL,
                MM_METERID TEXT,
                MM_METINSTD INTEGER,
                MM_METSEQ TEXT,
                MM_MININT REAL,
                MM_MINTOT REAL,
                MM_MMULT REAL,
                MM_MREADS TEXT,
                MM_NDIALS INTEGER,
                MM_NRKDIALS INTEGER,
                MM_NRKVARH TEXT,
                MM_NRKVMULT REAL,
                MM_NRKVSN TEXT,
                MM_OMITUPLD TEXT,
                MM_OPTDATA TEXT,
                MM_PCTCHG REAL,
                MM_PFTOL REAL,
                MM_PHASE INTEGER,
                MM_PMULT REAL,
                MM_POFFS REAL,
                MM_PTRATIO REAL,
                MM_PYSCHAN INTEGER,
                MM_RDCHAN TEXT,
                MM_RDTOL REAL,
                MM_RECID TEXT,
                MM_REGTYPE TEXT,
                MM_SERVTYPE TEXT,
                MM_TOLPCT REAL,
                MM_TOLTYPE TEXT,
                MM_TOTPULSE REAL,
                MM_UMCODE INTEGER,
                MM_UOMSCALE TEXT,
                MM_VCLC REAL,
                MM_VILC REAL,
                MM_VOLTS REAL,
                MM_WCLC REAL,
                MM_WILC REAL,
                MM_XFACCT TEXT,
                MM_ZEROINT REAL,
                MM_Extra1 TEXT,
                MM_Extra2 TEXT,
                MM_Extra3 TEXT
            )
         ''')
        db.commit()
    else:
        print('MF_Channel table already exists')


    db.close()
