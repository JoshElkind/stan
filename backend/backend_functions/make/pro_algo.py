import pandas as pd 
import numpy as np

def export_algorithm_to_file(outer_consts, row_wise_consts, deciders, user_id, function_name, filename):
    
    # Convert parameters to properly formatted string representations
    outer_consts_str = repr(outer_consts)
    row_wise_consts_str = repr(row_wise_consts)
    deciders_str = repr(deciders)
    
    # Create the Python code as a string
    python_code = f'''import pandas as pd
import numpy as np
import math

def {function_name}(df):
    outer_consts = {outer_consts_str}
    row_wise_consts = {row_wise_consts_str}
    deciders = {deciders_str}
    user_id = {user_id}
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
    outer_consts_store = {{}}
    
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
                final += (values[0] / values[1])
            elif combine_func == "//":
                final += (values[0] // values[1])
            elif combine_func == "^/":
                final += math.ceil(values[0] / values[1])
            elif combine_func == "^":
                final += (values[0] ** values[1])
            elif combine_func == "%":
                final += (values[0] % values[1])
            outer_consts_store[name] = int(final)
    
    for u in range(len_df):
        trw_consts = outer_consts_store.copy()
        trw_consts["Open"] = open.iloc[u]
        trw_consts["High"] = high.iloc[u]
        trw_consts["Low"] = low.iloc[u]
        trw_consts["Close"] = close.iloc[u]
        trw_consts["Volume"] = volume.iloc[u]
       
        
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
                    final += (values[0] / values[1])
                elif combine_func == "//":
                    final += (values[0] // values[1])
                elif combine_func == "^/":
                    final += math.ceil(values[0] / values[1])
                elif combine_func == "^":
                    final += (values[0] ** values[1])
                elif combine_func == "%":
                    final += (values[0] % values[1])
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
                    row_wise_consts_store = trw_consts.copy()
                    defining_var = curr_rwc[2][3]
                    row_wise_consts_store["Current_Row"] = w + 1
                    row_wise_consts_store["Window_Open"] = open.iloc[w]
                    row_wise_consts_store["Window_High"] = high.iloc[w]
                    row_wise_consts_store["Window_Low"] = low.iloc[w]
                    row_wise_consts_store["Window_Close"] = close.iloc[w]
                    row_wise_consts_store["Window_Volume"] = volume.iloc[w]
                    for r in range(len(curr_rwc[2][2])):
                        curr_row_wise_var = curr_rwc[2][2][r]
                        name_inner = curr_row_wise_var[1]
                        type_inner = curr_row_wise_var[0]
                        if type_inner == 0:
                            row_wise_consts_store[name_inner] = int(curr_row_wise_var[2])
                        if type_inner == 1:
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
                                final += (values[0] / values[1])
                            elif combine_function_rows == "//":
                                final += (values[0] // values[1])
                            elif combine_func_sub == "^/":
                                final += math.ceil(values[0] / values[1])
                            elif combine_func_sub == "^":
                                final += (values[0] ** values[1])
                            elif combine_func_sub == "%":
                                final += (values[0] % values[1])
                            row_wise_consts_store[name_inner] = int(final)
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
        
        num_buy = 0
        num_sell = 0
        len_deciders = len(deciders)
        for q in range(len_deciders):
            curr_decider = deciders[q]
            all_need = curr_decider[1]
            if curr_decider[0] == "Buy":
                curr_action = "Buy"
            else:
                curr_action = "Sell"
            num_false = 0
            for b in range(len(all_need)):
                curr_decider_single = all_need[b]
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

'''
    
    # we write code to file here...
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(python_code)
    
    print("Algorithm exported successfully to filename")


'''
outer_consts = [[0, "const_5", 5]]
row_wise_consts = []
deciders = [["Buy", [[[1,0], ["const_5", ">=", 1]]]]]  # Buy when const_5 >= 0
user_id = 54545
function_name = "my_trading_algo"
export_algorithm_to_file (
    outer_consts=outer_consts,
    row_wise_consts=row_wise_consts, 
    deciders=deciders,
    user_id=user_id,
    function_name=function_name,
    filename="exported_trading_algorithm.py"
)


'''