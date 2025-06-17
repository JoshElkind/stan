from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
from sqlalchemy import asc, desc, text
import pandas as pd
import re
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import Table, MetaData, insert
from .. import evaluationTools
import time
sys.path.append(os.path.dirname(__file__))
import k8s_runner
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import Window  
from pyspark.sql.functions import (
    lag,
    lead,
    avg,
    max,
    coalesce,
    lit,
    sum,
    array,
    col,
    abs,
    when,
    monotonically_increasing_id,
)
from pyspark.sql.types import IntegerType
from collections import Counter
import numpy as np  # used for time efficiency to copy column values into numpy arr, quicker than normal arr

load_dotenv()
print("🔐 DB_PASSWORD = ", os.getenv("DB_PASSWORD"))



def get_tablename_from_stocktype(stocktype):
    print(1)
    db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD')}@mydb-stan.c2t2eae8yvsz.us-east-1.rds.amazonaws.com:5432/stock-data"
    engine = create_engine(db_url)
    query = text('SELECT tablename FROM "StocksMain" WHERE stocktype = :stocktype')

    with engine.connect() as conn:
        result = conn.execute(query, {"stocktype": stocktype}).fetchone()
        if result:
            return result[0]
        else:
            return None
        

def get_asset_data(asset_name):
    print(2)
    print("Fetching type " + str(asset_name) + " asset tables for tests.")
    db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD')}@mydb-stan.c2t2eae8yvsz.us-east-1.rds.amazonaws.com:5432/stock-data"
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
        if 1 == 1:  # can add condiition later on to only select certain times...
            arr_dfs.append([arr_tablename.iloc[i], arr_tablename.iloc[i]])

    return arr_dfs


def get_asset_fullname(name):
    load_dotenv()
    print("🔐 DB_PASSWORD = ", os.getenv("DB_PASSWORD"))
    print(3)
    db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD')}@mydb-stan.c2t2eae8yvsz.us-east-1.rds.amazonaws.com:5432/stock-data"
    engine = create_engine(db_url)
    print("through engine")
    query = text('SELECT fullname FROM "FullNameReferences" WHERE name = :name')
    with engine.connect() as conn:
        result = conn.execute(query, {"name": name}).fetchone()
        if result:
            print(result)
            return result[0]
        else:
            print("NONE")
            return None


def clean_clusters(df, range_size):  # call with 1 and up (zero not valid for range...)
    print("Cleaning clusters.")
    length_df = len(df)
    arr_rmv = []
    for i in range(length_df):
        action = df.iloc[i]
        if action != "Hold":
            next_start = i + 1
            while next_start <= i + (range_size - 1) and next_start <= length_df - 1:
                next_start_action = df.iloc[next_start]
                if next_start_action == "Sell" and action == "Buy":
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                elif next_start_action == "Buy" and action == "Sell":
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                next_start += 1
    for i in range(len(arr_rmv)):
        print(arr_rmv[i])
        df.iloc[arr_rmv[i]] = "Hold"
    return df


def clean_clusters_arr_format(
    df, range_size
):  # call with 1 and up (zero not valid for range...)
    print("Cleaning Algo Clusters.")
    length_df = len(df)
    print(length_df)
    arr_rmv = []
    for i in range(length_df):
        action = df[i]
        if action != "Hold":
            next_start = i + 1
            while next_start <= i + (range_size - 1) and next_start <= length_df - 1:
                next_start_action = df[next_start]
                if next_start_action == "Sell" and action == "Buy":
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                elif next_start_action == "Buy" and action == "Sell":
                    arr_rmv.append(next_start)
                    arr_rmv.append(i)
                next_start += 1
    for i in range(len(arr_rmv)):
        df[arr_rmv[i]] = "Hold"
    print(df)
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
        elif Buy_count > 0:
            default_df.iloc[i] = "Buy"
            actions_track.append(action_track)
        elif Sell_count > 0:
            default_df.iloc[i] = "Sell"
            actions_track.append(action_track)

    default_df = clean_clusters(default_df, intercept_range)

    for i in range(num_rows):
        if default_df.iloc[i] == "Hold":
            actions_track[i] = []

    row_cur = 0
    all_matching_set = []
    verdict_actions = []
    while row_cur < num_rows:
        curr_action = default_df.iloc[row_cur]
        for i in range(len(all_matching_set)):
            all_matching_set[i][0] -= 1
        while len(all_matching_set) != 0 and all_matching_set[0][0] == 0:
            del all_matching_set[0]
        if curr_action != "Hold":
            all_matching_set.append(
                [intercept_range - 1, row_cur, actions_track[row_cur]]
            )
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


def sliding_verdict(actions, pos_idx, pos_flat, intercept, min_unique):
    N = len(actions)
    verdict = [2] * N
    counter = Counter()
    unique_count = 0
    for i in range(N):
        if actions[i] != 2:
            for k in range(pos_idx[i], pos_idx[i + 1]):
                p = pos_flat[k]
                counter[p] += 1
                if counter[p] == 1:
                    unique_count += 1
        j = i - intercept
        if j >= 0 and actions[j] != 2:
            for k in range(pos_idx[j], pos_idx[j + 1]):
                p = pos_flat[k]
                counter[p] -= 1
                if counter[p] == 0:
                    unique_count -= 1
                    del counter[p]
        if actions[i] != 2 and unique_count >= min_unique:
            verdict[i] = actions[i]
        else:
            verdict[i] = 2
    return verdict


def algos_combine_arr_format(post_algos, intercept_range, intercept_needed, num_rows):
    print("1Combining algos for single asset.")
    actions_track = []
    default_df = post_algos[0]
    print("2Combining algos for single asset.")
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
        elif Buy_count > 0:
            default_df[i] = "Buy"
            actions_track.append(action_track)
        elif Sell_count > 0:
            default_df[i] = "Sell"
            actions_track.append(action_track)

    default_df = clean_clusters_arr_format(default_df, intercept_range)

    print("Starting Sliding Window")

    for i in range(num_rows):
        if default_df[i] == "Hold":
            actions_track[i] = []

    action_map = {"Buy": 0, "Sell": 1, "Hold": 2}
    actions = [action_map[x] for x in default_df]

    pos_flat = []
    pos_idx = [0]
    for lst in actions_track:
        pos_flat.extend(lst)
        pos_idx.append(len(pos_flat))

    verdict = sliding_verdict(
        actions, pos_idx, pos_flat, intercept_range, intercept_needed
    )

    reverse_map = {0: "Buy", 1: "Sell", 2: "Hold"}
    for i in range(num_rows):
        default_df[i] = reverse_map[verdict[i]]

    return default_df


def algos_combine_arr_format_slow(
    post_algos, intercept_range, intercept_needed, num_rows
):
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
        elif Buy_count > 0:
            default_df[i] = "Buy"
            actions_track.append(action_track)
        elif Sell_count > 0:
            default_df[i] = "Sell"
            actions_track.append(action_track)

    default_df = clean_clusters_arr_format(default_df, intercept_range)

    print("Starting Sliding Window")

    for i in range(num_rows):
        if default_df[i] == "Hold":
            actions_track[i] = []

    row_cur = 0
    all_matching_set = []
    verdict_actions = []
    while row_cur < num_rows:
        curr_action = default_df[row_cur]
        for i in range(len(all_matching_set)):
            all_matching_set[i][0] -= 1
        while len(all_matching_set) != 0 and all_matching_set[0][0] == 0:
            del all_matching_set[0]
        if curr_action != "Hold":
            all_matching_set.append(
                [intercept_range - 1, row_cur, actions_track[row_cur]]
            )
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

def extract_func_name(algo_name):
        # Try to find between slash and .py
        match = re.search(r"(?:.*/)?(.*?)\.py$", algo_name)
        if not match:
            raise ValueError(f"Invalid algorithm name: {algo_name}")
        return match.group(1)

def asset_test_single(
    asset_table_name,
    postition_length,
    gain_percentage,
    loss_percentage,
    algo_names,
    intercept_range,
    clean_range,
    intercept_needed,
):
    print("✅ asset_test_single was invoked")
    spark = (
        SparkSession.builder.appName("converter")
        .config("spark.driver.memory", "12g")
        .config("spark.executor.memory", "12g")
        .config("spark.driver.cores", "6")
        .config("spark.executor.cores", "6")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    # Fetch the actual asset data from the database
    db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD')}@mydb-stan.c2t2eae8yvsz.us-east-1.rds.amazonaws.com:5432/stock-data"
    engine = create_engine(db_url)
    
    # Query the specific table to get the asset data
    query = text(f'SELECT * FROM "{asset_table_name}"')
    data_single_asset = pd.read_sql(query, engine)
    
    # Filter to only the columns we need
    data_single_asset = data_single_asset[["date", "open", "close"]]

    post_algos = []

    for j, algo_name in enumerate(algo_names):
        print(f"Applying Algo {j + 1}")
        try:
            procc = extract_func_name(algo_name)
            
            # RIGHT UNDER HERE, k8s_runner call!!!
            print("✅ about too call ")
            result = k8s_runner.run_algo_k8s(
                s3_key=algo_name,
                func_name=procc,
                table_name=asset_table_name
            )
            print("✅ called it ")
            cleaned_result = clean_clusters_arr_format(result, clean_range)
            print("worked!!!!")
            post_algos.append(cleaned_result)

        except Exception as e:
            print(f"Error running algorithm {procc}: {e}")
            continue

    print(post_algos)
    print(len(post_algos))
    print("ho")
    print(post_algos[0])
    print(len(post_algos[0]))
    print("hi")
    print(post_algos[0][0])
    print(len(post_algos[0][0]))
    print("hp")
    print(len(post_algos))
    print("a")
    print(intercept_range)
    print("b")
    print(min(len(post_algos), intercept_needed))
    print("c")
    print(len(post_algos[0]))
    print("d")
    
    combined_algos = algos_combine_arr_format(
        post_algos,
        intercept_range,
        min(len(post_algos), intercept_needed),
        len(post_algos[0]),
    )
    print("2worked!!!!")

    # Now create the Spark DataFrame from the fetched data
    data_single_asset = spark.createDataFrame(data_single_asset)
    data_single_asset = data_single_asset.withColumn(
        "row_id", monotonically_increasing_id().cast(IntegerType())
    )
    print("3worked!!!!")

    action_df = spark.createDataFrame(
        [(i, combined_algos[i]) for i in range(len(combined_algos))],
        ["row_id", "Action"],
    )
    print("4worked!!!!")
    
    data_single_asset = data_single_asset.join(action_df, on="row_id", how="inner")
    data_single_asset = data_single_asset.withColumn(
        "price", (col("open") + col("close")) / 2
    )
    print("5worked!!!!")
    
    data_single_asset = data_single_asset.select("date", "price", "Action")

    print("Evaluating combined algorithms' efficiency.")
    test = evaluationTools.test3(
        data_single_asset, postition_length, gain_percentage, loss_percentage
    )
    print("6worked!!!!")
    spark.stop()
    return test

def call_assets(
    arr_assets,
    postition_length,
    gain_percentage,
    loss_percentage,
    algos,
    intercept_range,
    clean_range,
    intercept_needed,
):
   
    final_results = []
    print("ARR assets" + str(arr_assets))
    for i in range(len(arr_assets)):
        asset_result = [[], []]
        curr_asset = arr_assets[i]
        curr_asset_fullname = get_asset_fullname(curr_asset)
        curr_asset_data = get_asset_data(curr_asset)
        print(curr_asset_fullname)
        print(curr_asset_data)
        print("Hi")
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
            # print("Asset " + str(j + 1) + " test proccess start.")
            time.sleep(1)
            # print("1fine!")
            result_single = asset_test_single(
                curr_table_df,
                postition_length,
                gain_percentage,
                loss_percentage,
                algos,
                intercept_range,
                clean_range,
                intercept_needed,
            )
            
            ### has %change, num buy wins, num buy loses, num sell wins, num sell loses, num buy actions, num sell actions
            
            total_percent_change += result_single[0]
            total_buy_wins += result_single[1]
            total_buy_loses += result_single[2]
            total_buy_actions += result_single[5]
            total_sell_wins += result_single[3]
            total_sell_loses += result_single[4]
            total_sell_actions += result_single[6]
            result_single.insert(0, curr_tablename)
            ### result_single now has tablename, %change, num buy wins, num buy loses, num sell wins, num sell loses, num buy actions, num sell actions, and the asset df with actions
            asset_result[1].append(result_single)
        asset_result[0].append(curr_asset)
        asset_result[0].append(curr_asset_fullname)
        asset_result[0].append(total_percent_change / avg_divide)
        asset_result[0].append(total_buy_wins)
        asset_result[0].append(total_buy_loses)
        asset_result[0].append(total_sell_wins)
        asset_result[0].append(total_sell_loses)
        asset_result[0].append(total_buy_actions)
        asset_result[0].append(total_sell_actions)
        final_results.append(asset_result)
    return final_results

