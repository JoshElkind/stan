from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, DateTime 
from sqlalchemy.orm import declarative_base, sessionmaker, relationship 
from datetime import datetime
from sqlalchemy import asc, desc, text
import pandas as pd 
import os 
from sqlalchemy import Table, MetaData, insert 
import evaluationTools

def get_tablename_from_stocktype(stocktype):
    db_url = 'postgresql://postgres@localhost:5432/stock-data' 
    engine = create_engine(db_url)  
    query = text('SELECT tablename FROM "StocksMain" WHERE stocktype = :stocktype')
    with engine.connect() as conn:
        result = conn.execute(query, {'stocktype': stocktype}).fetchone()
        if result:
            return result[0]  
        else:
            return None

def get_asset_data(asset_name):
    db_url = 'postgresql://postgres@localhost:5432/stock-data' 
    engine = create_engine(db_url)  
    Local_session = sessionmaker(bind=engine)
    local_session = Local_session() 
    Base = declarative_base()
    metadata = MetaData()

    table_assets = get_tablename_from_stocktype(asset_name)
    query = f'SELECT * FROM {(table_assets)}'
    df = pd.read_sql(query, engine)

    arr_dfs = []
    number_of_assets = len(df)
    arr_ticker = df["ticker"]
    arr_timetick = df["timetick"]
    arr_tablename = df["tablename"]
    for i in range(number_of_assets):
        if (1 == 1): # can add condiition later on to only select certain times...
            query = f'SELECT * FROM {(arr_tablename.iloc[i])}' # add each asset of that type 
            df_datacore = pd.read_sql(query, engine)
            arr_dfs.append(df_datacore)

    return arr_dfs

def get_asset_fullname(name):
    db_url = 'postgresql://postgres@localhost:5432/stock-data' 
    engine = create_engine(db_url)  
    query = text('SELECT fullname FROM "FullNameReferences" WHERE name = :name')
    with engine.connect() as conn:
        result = conn.execute(query, {'name': name}).fetchone()
        if result:
            return result[0]  
        else:
            return None

        
def call_assets(arr_assets, postition_length, gain_percentage, loss_percentage, algos, intercept_range, clean_range, intercept_needed):
    trips = []
    for i in range(len(arr_assets)):
        trip = [arr_assets[i], get_asset_fullname(arr_assets[i])]
        trip.append(get_asset_data(arr_assets[i]))    
        trips.append(trip)
    return asset_test_multiple(trips, postition_length, algos, intercept_range, clean_range,intercept_needed)
    
def clean_clusters(df, range):
    length = len(df)
    arr_rmv = []
    for i in range(length):
        action = df["Action"].iloc[i]
        if action != "Hold":
            next_start = i + 1
            while(next_start <= i + range and next_start <= length - 1):
                next_start_action = df["Action"].iloc[next_start]
                if (next_start_action == "Sell" and action == "Buy"):
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                elif (next_start_action == "Buy" and action == "Sell"):
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                next_start += 1
    for i in range(len(arr_rmv)):
        df.iloc[i, df.columns.get_loc("Action")] = "Hold"
    return df

def algos_combine(post_algos, intercept_range, intercept_needed, num_rows): 
    # intercept range is the ammount of tick spand that we need to find intercept_needed overlaps with the algos
    current_state = "Hold"
    track_reach = []
    for i in range(len(post_algos)):
        track_reach.append(0)
        



def asset_test_multiple(arr_assets_trip, postition_length, gain_percentage, loss_percentage, algos, intercept_range, clean_range, intercept_needed):
    tests_arr = []
    index_of_df_trip = 2
    index_of_fullname = 1
    index_of_nameasset = 0
    for i in range(len(arr_assets_trip)):
        data_single_asset = arr_assets_trip[i]
        post_algos = []
        for j in range(len(algos)):
            post_algos.append(clean_clusters(algos[j](data_single_asset[index_of_df_trip])))
        combined_algos = algos_combine(post_algos, intercept_range, min(len(algos), intercept_needed), len(data_single_asset[index_of_df_trip]))
        test = evaluationTools.test2(clean_clusters(combined_algos), postition_length, gain_percentage, loss_percentage) 
        test_whole = [data_single_asset[index_of_nameasset], data_single_asset[index_of_fullname, test]]
        tests_arr.append(test_whole)
    return tests_arr
