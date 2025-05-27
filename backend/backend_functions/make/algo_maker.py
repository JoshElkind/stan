import pandas as pd 
import numpy as np
# structure:

### FIRST ###
# outer-consts: array of [value_type, "var_name", value]
# value types: 
# 1. const (value_type = 0) - value will be a direct constant integer
# 2. reliant_const (1) - value will be an arr with:
# index 1 being another array containing 0 or 1 representing if first and second position values are const or variables,
# index 2 being the array containing const values themsleves or variable values
# index 3 being array combining method (+, -, *, /, ...) for values (length of this array is always one less than index 2 array)
# 3. row_calculated (2) - will be calculated with some kind of row operation on df, will be an array:
# index 1 being combining function (sum, avg so far...) to tell how to combine all row values
# index 2 being array (two values), start row and end row for the operation
# index 3 being an array an array of #1 or #2 value types, but the only difference
# is that the #2 value types arr index 1 will also allow a type 2 that represents the value of a certain 
# column on that row, also we will allow those specific row column values to be placed 
# as constants (AKA type #1)
# index 4 being name of variable in which we want to spit out to the combining function on each row


### SECOND ###
# row-wise-consts: array of [value_type, "var_name", value]
# value types (very similar as outer, except reset at each new row, and #1 is 
# direct row val instead of const, since we can just do const in the outer vars): 
# 1. reliant_const (value_type = 0) - value will be an arr with:
# index 1 being another array containing 0 or 1 representing if second position values are const or variables,
# index 2 being the array containing const value themsleves or variable values
# index 3 being array combining method (+, -, *, /, ...) for values (length of this array is always one less than index 2 array)
# 2. row_calculated (1) - will be calculated with some kind of row operation on df, will be an array:
# index 1 being combining function (sum, avg so far...) to tell how to combine all row values
# index 2 being array (two values), start row and end row for the operation
# index 3 being an array an array of #1 or #2 value types, but the only difference
# is that the #2 value types arr index 1 will also allow a type 2 that represents the value of a certain 
# column on that row, also we will allow those specific row column values to be placed 
# as constants (AKA type #1)
# index 4 being name of variable in which we want to spit out to the combining function on each row

### THIRD ###
# deciders: array of [const_bool, expression]
# index 1 being 0 ("Buy") or 1 ("Sell") indicating what this condition implies when satisfied
# index 2 being array of arrays of two bools that tell wether each side of expression (index 0,2) are literal const or vars
# index 3 being an array of arrays of three indexes:
#   first: value (const or var)
#   second: comparison (<, <=, >=, >, ==, !=)
#   third: value (const or var)

# USERS CAN ACCESS THE VAR CURRENT ROW WITH: current_row_const, THEY CAN ACCESS THE ACTUAL
# DESCISION ROW WITH: current_row

def generate_algo(outer_consts, row_wise_consts, deciders, user_id, function_name):
    def algo_code(df):
        print(user_id)
        len_df = len(df)
        actions_final = []
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date', ascending=True)
        open = df['open']
        high = df['high']
        low = df['low']
        close = df['close']
        volume = df['volume']
        outer_consts_store = {}
        for i in range(len(outer_consts)):
            curr = outer_consts[i]
            if curr[0] == 0:
                outer_consts_store[curr[1]] = int(curr[2])
            elif curr[0] == 1:
                name = curr[1]
                combine_func = curr[2][2]
                values = []
                if curr[2][0][0] == 0:
                    values.append(int(curr[2][1][0]))
                else:
                    values.append(int(outer_consts_store[curr[2][1][0]]))
                if curr[2][0][1] == 0:
                    values.append(curr[2][1][1])
                else:
                    values.append(outer_consts_store[curr[2][1][1]])
                final = 0
                if combine_func == "+":
                    final += (values[0] + values[1])
                elif combine_func == "-":
                    final += (values[0] - values[1])
                elif combine_func == "*":
                    final += (values[0] * values[1])
                elif combine_func == "/":
                    final += (values[0] * values[1])
                elif combine_func == "//":
                    final += (values[0] + values[1])
                elif combine_func == "^/":
                    final += (values[0] + values[1])
                elif combine_func == "^":
                    final += (values[0] + values[1])
                elif combine_func == "%":
                    final += (values[0] + values[1])
                outer_consts_store[name] = int(final)
            '''
            elif curr[0] == 2:
                row_vals = []
                name = curr[1]
                combining_func = curr[2][0]
                start_row_b = curr[2][1][0][0]
                end_row_b = curr[2][1][0][1]
                if start_row_b == 0:
                    start_row = curr[2][1][1][0]
                else:
                    start_row = outer_consts_store[curr[2][1][1][0]]
                if end_row_b == 0:
                    end_row = curr[2][1][1][1]
                else:
                    end_row = outer_consts_store[curr[2][1][1][1]]    
                for w in range(max(0,start_row - 1), min(end_row, len_df-1)):
                    row_wise_consts_store = outer_consts_store
                    defining_var = curr[2][3]
                    row_wise_consts_store["current_row_const"] = w + 1# available on client side (must indicate), represents row currently on 
                    row_wise_consts_store["current_open_const"] = open.iloc[w]
                    row_wise_consts_store["current_high_const"] = high.iloc[w]
                    row_wise_consts_store["current_low_const"] = low.iloc[w]
                    row_wise_consts_store["current_close_const"] = close.iloc[w]
                    row_wise_consts_store["current_volume_const"] = volume.iloc[w]
                    for r in range(len(curr[2][2])):
                        curr_row_wise_var = curr[2][2][r]
                        name = curr_row_wise_var[1]
                        type = curr_row_wise_var[0]
                        if type == 0:
                            row_wise_consts_store[name] = int(curr_row_wise_var[2])
                        if type == 1:
                            values = []
                            combine_func_sub = curr_row_wise_var[2][2]
                            if curr_row_wise_var[2][0][0] == 0:
                                values.append(int(curr_row_wise_var[2][1][0]))
                            else:
                                values.append(outer_consts_store[curr_row_wise_var[2][1][0]])
                            if curr_row_wise_var[2][0][1] == 0:
                                values.append(int(curr_row_wise_var[2][1][1]))
                            else:
                                values.append(outer_consts_store[curr_row_wise_var[2][1][1]])
                            final = 0
                            if combine_func_sub == "+":
                                final += (values[0] + values[1])
                            elif combine_func_sub == "-":
                                final += (values[0] - values[1])
                            elif combine_func_sub == "*":
                                final += (values[0] * values[1])
                            elif combine_func_sub == "/":
                                final += (values[0] * values[1])
                            elif combine_func_sub == "//":
                                final += (values[0] + values[1])
                            elif combine_func_sub == "^/":
                                final += (values[0] + values[1])
                            elif combine_func_sub == "^":
                                final += (values[0] + values[1])
                            elif combine_func_sub == "%":
                                final += (values[0] + values[1])
                            row_wise_consts_store[name] = int(final)
                    row_vals.append(row_wise_consts_store[defining_var])
                final_add = 0
                if combining_func == "avg":
                    final_add += (sum(row_vals) / len(row_vals))
                elif combining_func == "sum":
                    final_add += sum(row_vals)
                elif combining_func == "max":
                    final_add += max(row_vals)
                elif combining_func == "min":
                    final_add += min(row_vals)
                outer_consts_store[name] = final_add
        ''' # since we CAN'T peak into the future...
        ###############################################
        for u in range(len_df):
            trw_consts = outer_consts_store
            trw_consts["current_open"] = open.iloc[u]
            trw_consts["current_high"] = high.iloc[u]
            trw_consts["current_low"] = low.iloc[u]
            trw_consts["current_close"] = close.iloc[u]
            trw_consts["current_volume"] = volume.iloc[u]
            trw_consts["current_row"] = u + 1
            for l in range(len(row_wise_consts)):
                curr_rwc = row_wise_consts[l]
                name = curr_rwc[1]
                type = curr_rwc[0]
                if type == 0:
                    combine_func = curr_rwc[2][2]
                    values = []
                    if curr_rwc[2][0][0] == 0:
                        values.append(int(curr_rwc[2][1][0]))
                    else:
                        values.append(int(outer_consts_store[curr_rwc[2][1][0]]))
                    if curr_rwc[2][0][1] == 0:
                        values.append(curr_rwc[2][1][1])
                    else:
                        values.append(outer_consts_store[curr_rwc[2][1][1]])
                    final = 0
                    if combine_func == "+":
                        final += (values[0] + values[1])
                    elif combine_func == "-":
                        final += (values[0] - values[1])
                    elif combine_func == "*":
                        final += (values[0] * values[1])
                    elif combine_func == "/":
                        final += (values[0] * values[1])
                    elif combine_func == "//":
                        final += (values[0] + values[1])
                    elif combine_func == "^/":
                        final += (values[0] + values[1])
                    elif combine_func == "^":
                        final += (values[0] + values[1])
                    elif combine_func == "%":
                        final += (values[0] + values[1])
                    trw_consts[name] = int(final)
                elif type == 1:
                    row_vals = []
                    combine_function_rows = curr_rwc[2][0]
                    start_row_index_back = curr_rwc[2][1][1][0] - 1
                    if curr_rwc[2][1][0][0] == 1:
                        start_row_index_back = trw_consts[start_row_index_back]
                    end_row_index_back = curr_rwc[2][1][1][1] - 1
                    if curr_rwc[2][1][0][1] == 1:
                        end_row_index_back = trw_consts[end_row_index_back]
                    for w in range(max(0,u - start_row_index_back), min(u - (end_row_index_back + 1), u + 1)):
                        row_wise_consts_store = trw_consts
                        defining_var = curr_rwc[2][3]
                        row_wise_consts_store["current_row_const"] = w + 1# available on client side (must indicate), represents row currently on 
                        row_wise_consts_store["current_open_const"] = open.iloc[w]
                        row_wise_consts_store["current_high_const"] = high.iloc[w]
                        row_wise_consts_store["current_low_const"] = low.iloc[w]
                        row_wise_consts_store["current_close_const"] = close.iloc[w]
                        row_wise_consts_store["current_volume_const"] = volume.iloc[w]
                        for r in range(len(curr_rwc[2][2])):
                            curr_row_wise_var = curr_rwc[2][2][r]
                            name = curr_row_wise_var[1]
                            type = curr_row_wise_var[0]
                            if type == 0:
                                row_wise_consts_store[name] = int(curr_row_wise_var[2])
                            if type == 1:
                                values = []
                                combine_func_sub = curr_row_wise_var[2][2]
                                if curr_row_wise_var[2][0][0] == 0:
                                    values.append(int(curr_row_wise_var[2][1][0]))
                                else:
                                    values.append(outer_consts_store[curr_row_wise_var[2][1][0]])
                                if curr_row_wise_var[2][0][1] == 0:
                                    values.append(int(curr_row_wise_var[2][1][1]))
                                else:
                                    values.append(outer_consts_store[curr_row_wise_var[2][1][1]])
                                final = 0
                                if combine_func_sub == "+":
                                    final += (values[0] + values[1])
                                elif combine_func_sub == "-":
                                    final += (values[0] - values[1])
                                elif combine_func_sub == "*":
                                    final += (values[0] * values[1])
                                elif combine_func_sub == "/":
                                    final += (values[0] * values[1])
                                elif combine_function_rows == "//":
                                    final += (values[0] + values[1])
                                elif combine_func_sub == "^/":
                                    final += (values[0] + values[1])
                                elif combine_func_sub == "^":
                                    final += (values[0] + values[1])
                                elif combine_func_sub == "%":
                                    final += (values[0] + values[1])
                                row_wise_consts_store[name] = int(final)
                        row_vals.append(row_wise_consts_store[defining_var])
                    final_add = 0
                    if combine_function_rows == "avg":
                        final_add += (sum(row_vals) / len(row_vals))
                    elif combine_function_rows == "sum":
                        final_add += sum(row_vals)
                    elif combine_function_rows == "max":
                        final_add += max(row_vals)
                    elif combine_function_rows == "min":
                        final_add += min(row_vals)
                    trw_consts[name] = final_add
# index 1 being combining function (sum, avg so far...) to tell how to combine all row values
# index 2 being array (two values), start row and end row for the operation, ADDED ADDIOTNAL ARR AT FRONT TO SAY IF VARS OR NOT
# index 3 being an array an array of #1 or #2 value types, but the only difference
# is that the #2 value types arr index 1 will also allow a type 2 that represents the value of a certain 
# column on that row, also we will allow those specific row column values to be placed 
# as constants (AKA type #1)
# index 4 being name of variable in which we want to spit out to the combining function on each row
            num_buy = 0
            num_sell = 0
            len_deciders = len(deciders)
            for q in range(len_deciders):
                curr_decider = deciders[q]
                # print(curr_decider)
                all_need = curr_decider[1]
                if curr_decider[0] == 0:
                    curr_action = "Buy"
                else:
                    curr_action = "Sell"
                num_false = 0
                for b in range(len(all_need)):
                    curr_decider_single = all_need[b] # arr: [bool,bool],[val1,opperator,val2]
                    # print(curr_decider_single)
                    operator_func = curr_decider_single[1][1]
                    values_compare = []
                    if curr_decider_single[0][0] == 0:
                        values_compare.append(int(curr_decider_single[1][0]))
                    else:
                        values_compare.append(trw_consts[curr_decider_single[1][0]])
                    if curr_decider_single[0][1] == 0:
                        values_compare.append(int(curr_decider_single[1][2]))
                    else:
                        values_compare.append(trw_consts[curr_decider_single[1][2]])
                    # now check t/f
                    if (operator_func == "<"):
                        if values_compare[0] >= values_compare[1]:
                            num_false += 1
                    elif (operator_func == "<="):
                        if values_compare[0] > values_compare[1]:
                            num_false += 1
                    elif (operator_func == ">"):
                        if values_compare[0] <= values_compare[1]:
                            num_false += 1
                    elif (operator_func == ">="):
                        if values_compare[0] < values_compare[1]:
                            num_false += 1
                    elif (operator_func == "=="):
                        if values_compare[0] != values_compare[1]:
                            num_false += 1
                    elif (operator_func == "!="):
                        if values_compare[0] == values_compare[1]:
                            num_false += 1
                if num_false == 0 and curr_action == "Buy":
                    num_buy += 1
                if num_false == 0 and curr_action == "Sell":
                    num_sell += 1
            if num_sell == 0 and num_buy == 0:
                actions_final.append("Hold")
            elif num_sell > 0 and num_buy > 0:
                actions_final.append("Hold")
            elif num_sell > 0:
                actions_final.append("Sell")
            elif num_buy > 0:
                actions_final.append("Buy")
        return actions_final
    return algo_code
        
#dataframe = pd.read_csv("tam.csv")
# final_algo = (generate_algo([[0, "const_5", 5]], [], [["Sell", [[[1,0], ["const_5", ">=", 0]]]]], 54545, "func1"))
# arr_descisions = final_algo(dataframe)
# print(arr_descisions)