from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, DateTime 
from sqlalchemy.orm import declarative_base, sessionmaker, relationship 
from datetime import datetime
from sqlalchemy import asc, desc, text
import pandas as pd 
import os 
from sqlalchemy import Table, MetaData, insert 
import evaluationTools
import time
from algorithms.test_algorithms import pivot_atr_algorithm, bollinger_bands_minute_strategy, macd_vwap_strategy, vwap_pullback_strategy, momentum_strategy, mean_reversion_strategy, breakout_strategy, hft_pattern_strategy, candlestick_pattern_strategy


import numpy as np # used for time efficiency to copy column values into numpy arr, quicker than normal arr


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
    print("Fetching type " + str(asset_name) + " asset tables for tests.")
    db_url = 'postgresql://postgres@localhost:5432/stock-data' 
    engine = create_engine(db_url)  
    Local_session = sessionmaker(bind=engine)
    local_session = Local_session() 
    Base = declarative_base()
    metadata = MetaData()

    table_assets = get_tablename_from_stocktype(asset_name)
    
    query = text(f'SELECT * FROM "{table_assets}"')
    df = pd.read_sql(query, engine)
    

    arr_dfs = []
    number_of_assets = len(df)
    arr_ticker = df["ticker"]
    arr_timetick = df["timetick"]
    arr_tablename = df["tablename"]
    for i in range(number_of_assets):
        if (1 == 1): # can add condiition later on to only select certain times... 
            query = text(f'SELECT * FROM "{(arr_tablename.iloc[i])}"') # add each asset of that type
            df_datacore = (pd.read_sql(query, engine))
            df_datacore['date'] = pd.to_datetime(df_datacore['date'])
            df_datacore = df_datacore.sort_values(by='date', ascending=True)
            arr_dfs.append([arr_tablename.iloc[i], df_datacore])

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

    
def clean_clusters(df, range_size): # call with 1 and up (zero not valid for range...)
    print("Cleaning clusters.")
    length_df = len(df)
    arr_rmv = []
    for i in range(length_df):
        action = df.iloc[i]
        if action != "Hold":
            next_start = i + 1
            while(next_start <= i + (range_size - 1) and next_start <= length_df - 1):
                next_start_action = df.iloc[next_start]
                if (next_start_action == "Sell" and action == "Buy"):
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                elif (next_start_action == "Buy" and action == "Sell"):
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                next_start += 1
    for i in range(len(arr_rmv)):
        print(arr_rmv[i])
        df.iloc[arr_rmv[i]] = "Hold"
    return df


def clean_clusters_arr_format(df, range_size): # call with 1 and up (zero not valid for range...)
    print("Cleaning Algo Clusters.")
    length_df = len(df)
    arr_rmv = []
    for i in range(length_df):
        action = df[i]
        if action != "Hold":
            next_start = i + 1
            while(next_start <= i + (range_size - 1) and next_start <= length_df - 1):
                next_start_action = df[next_start]
                if (next_start_action == "Sell" and action == "Buy"):
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                elif (next_start_action == "Buy" and action == "Sell"):
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                next_start += 1
    for i in range(len(arr_rmv)):
        df[arr_rmv[i]] = "Hold"
    return df

def count_unique_pos(all_matching_set):
        unique_occurences = set()
        for i in range(len(all_matching_set)):
            for j in range(len(all_matching_set[i][2])):
                unique_occurences.add(all_matching_set[i][2][j])
        return len(unique_occurences)
    

def algos_combine(post_algos, intercept_range, intercept_needed, num_rows): 
    # intercept range is the ammount of tick spand that we need to find intercept_needed overlaps with the algos
    actions_track = []
    default_df = (post_algos[0])["Action"]
    print("Combining algos for single asset.")
    time.sleep(1)

    for i in range(len(post_algos)):
        post_algos[i] = (post_algos[i])["Action"]

    for i in range(num_rows):
        Sell_count = 0
        Buy_count = 0
        action_track = []
        for j in range(len(post_algos)):
            if post_algos[j].iloc[i] == "Buy":
                Buy_count += 1
                action_track.append(j)
            elif post_algos[j].iloc[i] == "Sell":
                Sell_count += 1
                action_track.append(j)
        if (Sell_count != 0 and Buy_count != 0) or (Sell_count == 0 and Buy_count == 0):
            default_df.iloc[i] = "Hold"
            actions_track.append([])
        elif (Buy_count > 0):
            default_df.iloc[i] = "Buy"
            actions_track.append(action_track)
        elif (Sell_count > 0):
            default_df.iloc[i] = "Sell"
            actions_track.append(action_track)

    default_df = clean_clusters(default_df, intercept_range)

    for i in range(num_rows):
        if default_df.iloc[i] == "Hold":
            actions_track[i] = []
   
    row_cur = 0
    all_matching_set = []
    verdict_actions = []
    while (row_cur < num_rows):
        curr_action = default_df.iloc[row_cur]
        for i in range(len(all_matching_set)):
            all_matching_set[i][0] -= 1
        while (len(all_matching_set) != 0 and all_matching_set[0][0] == 0):
            del all_matching_set[0]
        if (curr_action != "Hold"):
            all_matching_set.append([intercept_range - 1, row_cur, actions_track[row_cur]])
            if count_unique_pos(all_matching_set) >= intercept_needed:
                for i in range(len(all_matching_set)):
                    verdict_actions.append([all_matching_set[i][1], curr_action])
        row_cur += 1    

    
    for i in range(num_rows):
        default_df.iloc[i] = "Hold"
    for j in range(len(verdict_actions)):
        default_df.iloc[verdict_actions[j][0]] = verdict_actions[j][1]

    post_algos[0]["Action"] = default_df

    return post_algos[0]


def algos_combine_arr_format(post_algos, intercept_range, intercept_needed, num_rows): 
    # intercept range is the ammount of tick spand that we need to find intercept_needed overlaps with the algos
    actions_track = []
    default_df = post_algos[0]
    print("Combining algos for single asset.")
    time.sleep(1)

    for i in range(num_rows):
        Sell_count = 0
        Buy_count = 0
        action_track = []
        for j in range(len(post_algos)):
            if post_algos[j][i] == "Buy":
                Buy_count += 1
                action_track.append(j)
            elif post_algos[j][i] == "Sell":
                Sell_count += 1
                action_track.append(j)
        if (Sell_count != 0 and Buy_count != 0) or (Sell_count == 0 and Buy_count == 0):
            default_df[i] = "Hold"
            actions_track.append([])
        elif (Buy_count > 0):
            default_df[i] = "Buy"
            actions_track.append(action_track)
        elif (Sell_count > 0):
            default_df[i] = "Sell"
            actions_track.append(action_track)

    default_df = clean_clusters_arr_format(default_df, intercept_range)

    for i in range(num_rows):
        if default_df[i] == "Hold":
            actions_track[i] = []
   
    row_cur = 0
    all_matching_set = []
    verdict_actions = []
    while (row_cur < num_rows):
        curr_action = default_df[row_cur]
        for i in range(len(all_matching_set)):
            all_matching_set[i][0] -= 1
        while (len(all_matching_set) != 0 and all_matching_set[0][0] == 0):
            del all_matching_set[0]
        if (curr_action != "Hold"):
            all_matching_set.append([intercept_range - 1, row_cur, actions_track[row_cur]])
            if count_unique_pos(all_matching_set) >= intercept_needed:
                for i in range(len(all_matching_set)):
                    verdict_actions.append([all_matching_set[i][1], curr_action])
        row_cur += 1    
    
    for i in range(num_rows):
        default_df[i] = "Hold"
    for j in range(len(verdict_actions)):
        default_df[verdict_actions[j][0]] = verdict_actions[j][1]

    # out = pd.DataFrame(default_df, columns=["Action"])
    # out.to_csv("out.csv", index=False)

    # for i in range(len(post_algos)):
        # out_sub = pd.DataFrame(post_algos[i], columns=["Action"])
        # out_sub.to_csv("out" + str(i + 1) + ".csv", index=False)
    return default_df



def count_actions(action, df):
    actions = df["Action"]
    count = 0
    for i in range(len(actions)):
        if actions.iloc[i] == action:
            count += 1
    return count

def count_actions_arr_format(action, df):
    count = 0
    for i in range(len(df)):
        if df[i] == action:
            count += 1
    return count


def asset_test_single(arr_asset, postition_length, gain_percentage, loss_percentage, algos, intercept_range, clean_range, intercept_needed):
    data_single_asset = arr_asset
    post_algos = []
    for j in range(len(algos)):
        print("Applying Algo " + str(j + 1))
        curr_algo = (algos[j](data_single_asset))
        new_actions = clean_clusters_arr_format(curr_algo["Action"].to_numpy(), clean_range)
        post_algos.append(new_actions)
    combined_algos = algos_combine_arr_format(post_algos, intercept_range, min(len(algos), intercept_needed), len(data_single_asset))
    data_single_asset["Action"] = combined_algos
    print("Evaluating combined algorithms' efficiency.")
    test = evaluationTools.test3(data_single_asset, postition_length, gain_percentage, loss_percentage) 
    print("Counting Buy/Sell Actions")
    num_buy_actions = count_actions_arr_format("Buy", combined_algos)
    num_sell_actions = count_actions_arr_format("Sell", combined_algos)
    test.append(num_buy_actions)
    test.append(num_sell_actions)
    return test

def call_assets(arr_assets, postition_length, gain_percentage, loss_percentage, algos, intercept_range, clean_range, intercept_needed):
    final_results = []
    for i in range(len(arr_assets)):
       asset_result = [[],[]]
       curr_asset = arr_assets[i]
       curr_asset_fullname = get_asset_fullname(curr_asset)
       curr_asset_data = get_asset_data(curr_asset)
       total_percent_change = 0
       total_buy_wins = 0
       total_buy_loses = 0
       total_buy_actions = 0
       total_sell_wins = 0
       total_sell_loses = 0
       total_sell_actions = 0
       avg_divide = len(curr_asset_data)
       for j in range(len(curr_asset_data)):
            curr_tablename = curr_asset_data[j][0]
            curr_table_df = curr_asset_data[j][1]
            print("Asset " + str(j + 1) + " test proccess start.")
            time.sleep(1)
            result_single = asset_test_single(curr_table_df, postition_length, gain_percentage, loss_percentage, algos, intercept_range, clean_range, intercept_needed)
            ### has %change, num buy wins, num buy loses, num sell wins, num sell loses, num buy actions, num sell actions
            total_percent_change += result_single[0]
            total_buy_wins += result_single[1]
            total_buy_loses += result_single[2]
            total_buy_actions += result_single[5]
            total_sell_wins += result_single[3]
            total_sell_loses += result_single[4]
            total_sell_actions += result_single[6]
            result_single.insert(0, curr_tablename)
            result_single.append(curr_table_df.iloc[100:200]) # for a preview we copy over 100 ticks of df (rows 100 to 200)
            ### result_single now has tablename, %change, num buy wins, num buy loses, num sell wins, num sell loses, num buy actions, num sell actions, and the asset df with actions
            asset_result[1].append(result_single)
       asset_result[0].append(curr_asset)
       asset_result[0].append(curr_asset_fullname)
       asset_result[0].append(total_percent_change/avg_divide)
       asset_result[0].append(total_buy_wins)
       asset_result[0].append(total_buy_loses)
       asset_result[0].append(total_sell_wins)
       asset_result[0].append(total_sell_loses)
       asset_result[0].append(total_buy_actions)
       asset_result[0].append(total_sell_actions)
       final_results.append(asset_result)
    return final_results
    
'''
df1 = pd.read_csv("algorithms/results1.csv")
print("_!_")
print(count_actions("Sell", df1))
print(count_actions("Buy", df1))
cleaned_actions = clean_clusters(df1["Action"],4)
df1["Action"] = cleaned_actions
print(count_actions("Sell", df1))
print(count_actions("Buy", df1))
df2 = df1

arr_1 = df1['Action'].to_numpy()
arr_2 = df1['Action'].to_numpy()

### print(algos_combine([df1,df2], 20, 2, len(df1)))

print("Check 0 -")
time.sleep(2.5) 

post_arr = algos_combine_arr_format([arr_1,arr_2], 20, 2, len(arr_1))
print(count_actions_arr_format("Buy", post_arr))
print(count_actions_arr_format("Sell", post_arr))
### print(count_actions("Sell", df3))
### print(count_actions("Buy", df3))
### df3.to_csv("combined_results.csv")
'''

# test1 = call_assets(['C'], 40, 0.02, 0.008, [vwap_pullback_strategy], 40, 3, 1) # -0.0031270911296279913
# test1 = call_assets(['C'], 30, 0.02, 0.008, [vwap_pullback_strategy, bollinger_bands_minute_strategy], 20, 3, 2) # -0.006326465479976327
# test1 = call_assets(['S'], 30, 0.02, 0.008, [mean_reversion_strategy, bollinger_bands_minute_strategy, vwap_pullback_strategy], 20, 3, 3) # -0.006535486899338375

test1 = call_assets(['C'], 40, 0.02, 0.02, [vwap_pullback_strategy], 40, 3, 1)
print(test1[0]) 