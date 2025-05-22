from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, DateTime 
from sqlalchemy.orm import declarative_base, sessionmaker, relationship # must import relationship for one-to-many
from datetime import datetime
from sqlalchemy import asc, desc

import pandas as pd 

import os # for files/directories

from sqlalchemy import Table, MetaData, insert, select
# used to insert into a table we only know name of and know form of cols but 
# we don't have static object schema


db_url = 'postgresql://postgres@localhost:5432/stock-data' 
engine = create_engine(db_url)  
Local_session = sessionmaker(bind=engine)
local_session = Local_session() 
Base = declarative_base()
metadata = MetaData()
### Needed to add to rows brute force method without having ORM class...

def initiate_stockdata_table(name):
    newtable = type(
        name,
        (Base,),
        {
            '__tablename__': name,
            '__table_args__': {'extend_existing': True},
            'date': Column(DateTime, primary_key=True),
            'open': Column(Float),
            'high': Column(Float),
            'low': Column(Float),
            'close': Column(Float),
            'volume': Column(Float),
        }
    )

    Base.metadata.create_all(bind=engine, tables=[newtable.__table__])
    return newtable


def initiate_stocktype_table(name):
    newtable = type(
        name,
        (Base,),
        {
            '__tablename__': name,
            '__table_args__': {'extend_existing': True},
            'ticker': Column(String),
            'timetick': Column(String),
            'tablename': Column(String, primary_key=True),
        }
    )

    Base.metadata.create_all(bind=engine, tables=[newtable.__table__])
    return newtable

def initiate_stocks_table(name):
    newtable = type(
        name,
        (Base,),
        {
            '__tablename__': name,
            '__table_args__': {'extend_existing': True},
            'stocktype': Column(String),
            'tablename': Column(String, primary_key=True),
        }
    )
    Base.metadata.create_all(bind=engine, tables=[newtable.__table__])
    return newtable



def push_csv(name, Table_Class, data_path, volume_multiplier):

    df = pd.read_csv(data_path) 
    df.dropna()
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(by='Date', ascending=True)
    number_rows = len(df)

    multiple_new_stockdata_rows = []

    df["Volume"] *= volume_multiplier
    loop_datetime = pd.to_datetime(df["Date"])
    loop_open = df["Open"]
    loop_high = df["High"]
    loop_low = df["Low"]
    loop_close = df["Close"]
    loop_volume = df["Volume"]

    for i in range(number_rows):
        multiple_new_stockdata_rows.append(
            Table_Class(
                date = loop_datetime.iloc[i],
                open = float(loop_open.iloc[i]),
                high = float(loop_high.iloc[i]),
                low = float(loop_low.iloc[i]),
                close = float(loop_close.iloc[i]),
                volume = float(loop_volume.iloc[i])
            )
        )

    local_session.add_all(multiple_new_stockdata_rows)

    try:
        local_session.commit()
    except Exception as e:
        local_session.rollback()
        print(f"An error occurred: {e}")

    '''
    parent_table_data = Table(name, metadata, autoload_with=engine)
    with engine.connect() as conn: 
        try:
            for i in range(number_rows):
                insertion_request = insert(parent_table_data).values(
                    date = loop_datetime.iloc[i],
                    open = float(loop_open.iloc[i]),
                    high = float(loop_high.iloc[i]),
                    low = float(loop_low.iloc[i]),
                    close = float(loop_close.iloc[i]),
                    volume = float(loop_volume.iloc[i]))
                conn.execute(insertion_request)
            conn.commit()
        except Exception as e:
            print(f"Insert failed: {e}")
    '''
   

def create_stockdata_table(stock_ticker, time_tick, stock_type, csv_data_path, volume_multiplier):
    # stock_type will be S (stock), C (crypto), etc...
    name = time_tick + "_" + stock_ticker
    tablename_new = initiate_stockdata_table(name)
    push_csv(name, tablename_new, csv_data_path, volume_multiplier)

    parent_name = stock_type + "Type"
    parent_table_type = Table(parent_name, metadata, autoload_with=engine)
    insertion_request = insert(parent_table_type).values(
        ticker = stock_ticker,
        timetick = time_tick,
        tablename = name)
    with engine.connect() as conn: 
    # different type of connection, not object oriented
    # more brute force since we are dealing with dynamically named tables
        conn.execute(insertion_request)
        conn.commit()
    

def create_stocktype_table(stock_type):
    name = stock_type + "Type"
    initiate_stocktype_table(name)
    parent_name = "StocksMain"
    parent_table_type = Table(parent_name, metadata, autoload_with=engine)
    with engine.connect() as conn:
        result = conn.execute(select(parent_table_type))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by='Date', ascending=True)
        print(df.head()) 
    insertion_request = insert(parent_table_type).values(
        stocktype=stock_type,
        tablename=name
    )
    with engine.connect() as conn:
        conn.execute(insertion_request)
        conn.commit()


def create_stocks_table():
    initiate_stocks_table("StocksMain")

def create_namereferences_table():
    class FullNameReferences(Base):
        __tablename__ = "FullNameReferences"
        name = Column(String, primary_key=True)
        fullname = Column(String)
    Base.metadata.create_all(bind=engine)
    return FullNameReferences


def push_stocktype_folder(stocktype, stockfullname, timetick, volume_multiplier, path_folder):
    create_stocktype_table(stocktype)
    References = create_namereferences_table()
    new_reference = References(
        name=stocktype,
        fullname=stockfullname
    )
    local_session.add(new_reference)
    local_session.commit()

    files = []
    for file_name in os.listdir(path_folder):
        full_path = os.path.join(path_folder, file_name)
        if os.path.isfile(full_path) and file_name.endswith('.csv'):
            name_without_ext = os.path.splitext(file_name)[0]
            files.append((name_without_ext, full_path))

    for file in files:
        create_stockdata_table(file[0], timetick, stocktype, file[1], volume_multiplier)

def push_stockdata_folder(stocktype, timetick, volume_multiplier, path_folder):
    files = []
    for file_name in os.listdir(path_folder):
        full_path = os.path.join(path_folder, file_name)
        if os.path.isfile(full_path) and file_name.endswith('.csv'):
            name_without_ext = os.path.splitext(file_name)[0]
            files.append((name_without_ext, full_path))

    for file in files:
        create_stockdata_table(file[0], timetick, stocktype, file[1], volume_multiplier)

def start_stockdb():
    create_stocks_table()


push_stocktype_folder("T", "Test", "1min", 1, "/Users/joshelkind/stan/backend/backend_functions/data")

# push_stocktype_folder("T", "Test", "1min", 1, "/Users/joshelkind/stan/backend/data")

### PUT SOME SHORT MIXTURE OF ASSETS in TTYPE ("T") and make them short 200 000 rows each (put 4 tables only)
### THIS WAY WE CAN DEMO ON WEBSITE TO USERS QUICKLY!!!