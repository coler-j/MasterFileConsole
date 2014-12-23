from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import SingletonThreadPool
import MasterParse as mp
 
from sqlalchemy_declarative import Customer, Channel, Recorder, Base

# Create an Enginge Point to Datasource 
engine = create_engine(r'sqlite:///C:\\Users\\CJANCSAR\\Documents\\FNBUG\\Data\\MV90Extension.db', poolclass=SingletonThreadPool)
# Bind the engine to the metadata of the Base class so that the
# declaratives can be accessed through a DBSession instance
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

# Parse all Master Files
masterFileLocation = r'C:\Users\CJANCSAR\Documents\FNBUG\ALL.DAT'
masterList = mp.parseMaster(masterFileLocation)

# Unpack Parse Records
recorderList = masterList[0]
customerList = masterList[1]
channelList = masterList[2]

#for recorder in recorderList:
 #   print(recorder.RM_RECID)

session.add_all([recorder for recorder in recorderList])
session.commit()
session.add_all([customer for customer in customerList])
session.commit()
session.add_all([channel for channel in channelList])
session.commit()


#for recorder in recorderList:
#    session.add(recorder)
#    session.commit()
 
# Insert a Person in the person table
#new_Customer = Customer(CM_CUSTID='TEST PERSON')
#session.add(new_Customer)
#session.commit()
 
