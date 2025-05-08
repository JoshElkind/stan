from sqlalchemy import create_engine

from sqlalchemy import Column, Integer, String, Float, DateTime # this imports the sql data types (not python ones...)

from sqlalchemy.orm import declarative_base, sessionmaker # this imports ORM tools
# ORM tools allow us to use Python classes and objects WITH SQL databases
# 1. declarative_base - basically simplifies SQL tables by making a class for them in Python
# 2. sessionmaker - allows us to initiate sessions with SQL databases, a session is essentialy
# a temporary conversation between our python code and the database that allows us to do things
# like Add Rows, Query Data, Commit Changes, etc...

from datetime import datetime # used for datetime column stuff

from sqlalchemy import asc, desc # sorting rows in ascending/descending order

db_url = 'postgresql://postgres@localhost:5432/stock-data' # api url to access the postgress db

engine = create_engine(db_url) 
# sets up a connection with db, engine is how we play with it

Local_session = sessionmaker(bind=engine)
local_session = Local_session() 
# makes a session within the connection, where we can add, query, commit data
# starts the actual session with Local_session(), then we reference local_session
# for any actions 

Base = declarative_base()
# whenever we create templates for new tables, we want to make it into a python class
# to do so we declare this basic Base, so that when we actually create new table 
# classes, they can inherit this Base

# ^^^
# lets use this Base class, to create our first python class table for stock data
class StockData(Base):
    __tablename__ = "stockdata"
    date = Column(DateTime, primary_key=True)
    ticker = Column(String)
    price = Column(Float)
### NOTE: primary_key basically ensures that all values within a column are unique
# and also ensures we can't create multiple of column with same name

### Base.metadata.create_all(bind=engine) # this creates and pushes all table classes to the db
# in our case, will create (if not already) a stockdata table in our postgress db

### ADDING TO TABLES IN DB ###

### NOTE: we cannot do any of below adding, unless the table has been created 
# already with the line 40

# lets below create a new row corresponding to the class of StockData,
# this means we must give the matching datetime, ticker, open attributes
new_stockdata_row = StockData(
    date=datetime(2015, 5, 3, 9, 30),
    ticker='MSFT',
    price=12.35
)
# now we need to add the row to our db, by doing .add() to our session, the PyAlchemy
# basically finds the corresponding table in our db matching to the class of our new row
# and adds the new row to the end of it
### local_session.add(new_stockdata_row)
### local_session.commit()
# NOTE: can't insert this specific row twice since we set date to be a unique key
# meaning can't have duplicates

# ADD MULTIPLE ROWS AT ONCE:
# to do so we just simply .add_all() INTEAD of .add() AND we use an array of the
# single class rows INSTEAD of a single instance:
multiple_new_stockdata_rows = [
    StockData(date=datetime(2025, 5, 3, 9, 31), ticker='AAPL', price=1),
    StockData(date=datetime(2025, 5, 3, 9, 32), ticker='AAPL', price=2),
    StockData(date=datetime(2025, 5, 3, 9, 33), ticker='AAPL', price=3)
]
### local_session.add_all(multiple_new_stockdata_rows) # adds changes
### local_session.commit() # comits then (AKA actually makes the changes/pushes)

### ERROR HANDLING ###
# before we commit the changes we can do:
try:
    local_session.commit()
except Exception as e:
    local_session.rollback()
    print(f"An error occurred: {e}")
finally:
    local_session.close()
# NOTE: can try it out by uncommenting any of the above adds since will cause an
# error with the needed uniqueness of date keys caused by trying to add multiple times
# same date...

# ADDING AT Nth ROW
# not possible in SQLAlchemy but we can add a certain position attribute to the rows
# that we can adjust to insert rows in middle positions, but this will require us to 
# query the db whenever we want to retrieve data with .order_by(asc(Object.position))
# to make sure we are mainting proper ordering


### FETCHING DATA/TABLES FROM DB ###
# can fetch data from any table in DB all at once by:
all_stockdata_rows = local_session.query(StockData).order_by(asc(StockData.date)).all() # we get an array of row stockdata objects
### NOTE: when we insert the rows, may not maintain same order in db, so if you want to fetch the rows in correct order
### must use some kind of .order_by(asc/desc(Object.attribute)) that way it is ordered...
# for row in all_stockdata_rows:
    # print(row.date, row.ticker, row.price) # access the attributes of the stockdata objects

# Other Stuff:
first_stock = local_session.query(StockData).first() # get first row

# To get the last row, it's more complicated, here we essentialy flip the ordering (reverse)
# by ordering the rows in descending order of date, then we call first on it:
last_stock = local_session.query(StockData).order_by(desc(StockData.date)).first() 

### FILTERING TO ONLY QUERY CERTAIN ROWS ###
apple_stocks_filtered = local_session.query(StockData).filter(StockData.ticker == 'AAPL').order_by(asc(StockData.date)).all()
# for row in apple_stocks_filtered:
    # print(row.date, row.ticker, row.price)
### NOTE: notice since we filtered to only APPL data MSFT (and more maybe) doesn't appear in the query

# 1.
# in order to chain multiple filters, just add the extra .filter() like below:
apple_stocks_filtered = local_session.query(StockData).filter(StockData.ticker == 'AAPL').filter(StockData.price == 3.0).order_by(asc(StockData.date)).all()
for row in apple_stocks_filtered:
    print(row.date, row.ticker, row.price) 

# 2.
# or we can also use the & operator to essentialy use AND, (note: can't use: and)
apple_stocks_filtered = local_session.query(StockData).filter((StockData.ticker == 'AAPL') & (StockData.price == 3.0)).order_by(asc(StockData.date)).all()
for row in apple_stocks_filtered:
    print(row.date, row.ticker, row.price) 

# CAN ADD ADDITIONAL OPTIONS TO QUERIES (NOTE: will see we can stack options in our queries):

recent_stocks = local_session.query(StockData).order_by(StockData.date.desc()).all() 
# will order stocks (rows) queried in latest to oldest order

apple_stocks_filtered = local_session.query(StockData).filter(StockData.ticker == 'AAPL').order_by(StockData.date.desc()).all()
# will order stocks (rows) queried in latest to oldest order AFTER we filter it to be only APPL stock (rows)


### FETCHING Nth INDEX ROW ###
# to do so we simply must query with some sort of ordering (.order_by()) to reflect the proper order of insertion
# this is usually done with (.order_by(asc(Object.id))) or with in our case can also do (.order_by(asc(StockData.date)))
# but adding an extra id field to our object really helps with these nth row type operations...
fourth_th_row = local_session.query(StockData).order_by(asc(StockData.date)).offset(3).limit(1).first()


### DELETING ROWS FROM DB ###

# to delete certain rows (that match our specifications):
### local_session.query(StockData).filter(StockData.ticker == 'AAPL').delete() # no need for ordering cuz we just want to get all then delete all...
### local_session.commit()
### query the rows you want, them we delete them 

# TO DELETE WHOLE TABLE (remove all rows):
### session.query(StockData).delete()
### session.commit()

### Given that we inserted in a sorted order in terms of date, we can delete the n-th
# row by querying with the .order_by(asc) option, and offset by n - 1 to get n - 1 index row:
### fourth_th_row = local_session.query(StockData).order_by(asc(StockData.date)).offset(3).limit(1).first()
### fourth_th_row.delete()
### local_session.commit()
# we can still technically not include .limit(1) and then just call first, but not very efficient cuz
# we still call all the remaining n indexed rows after the one we need, vs with the limit we only query the 
# row we need, stop, and then call .first() to directly access it...

### NOTE: we can fetch n rows after a certain offset or filter by calling .limit(n) 
# on top at end of previous options (but still always but .all() at the end)

### NOTE: if we don't have any columns that following ordering but we still want to 
# access the n-th we can probably just run .offset() the same as always without any
# .order_by, but if you want to be really safe, probably better to increment an id 
# and attach to each new inserted row that way we have some kind of extra ordering 
### SQLALCHEMY actually does this for you with the id attribute:
id = Column(Integer, primary_key=True) # add this column to your tables 
# now whenever you add new row just don't specify the id and sqlalchemy will automatically
# assign the next higher available id that way we maintain order while inserting to the db table...

