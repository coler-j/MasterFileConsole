from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
 
from sqlalchemy_declarative import Customer, Channel, Recorder, Base
 
engine = create_engine(r'sqlite:///C:\\Users\\CJANCSAR\\Documents\\FNBUG\\Data\\MV90Extension.db')
# Bind the engine to the metadata of the Base class so that the
# declaratives can be accessed through a DBSession instance
Base.metadata.bind = engine
 
DBSession = sessionmaker(bind=engine)
session = DBSession()
 
# Insert a Person in the person table
new_Customer = Customer(CM_CUSTID='TEST PERSON')
session.add(new_Customer)
session.commit()
 
