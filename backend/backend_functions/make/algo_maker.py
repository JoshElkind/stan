import pandas as pd 

# structure:

### FIRST ###
# outer-consts: array of [value_type, "var_name", value]
# value types: 
# 1. const (value_type = 0) - value will be a direct constant integer
# 2. reliant_const (1) - value will be an arr with:
# index 1 being another array containing 0 or 1 representing if second position values are const or variables,
# index 2 being the array containing const value themsleves or variable values
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
# 1. row-const (value_type = 0) - value will be name of column (string) to take row value from...
# 2. reliant_const (1) - value will be an arr with:
# index 1 being another array containing 0 or 1 representing if second position values are const or variables,
# index 2 being the array containing const value themsleves or variable values
# index 3 being array combining method (+, -, *, /, ...) for values (length of this array is always one less than index 2 array)
# 3. row_calculated (2) - will be calculated with some kind of row operation on df, will be an array:
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
# index 2 being array of two bools that tell wether each side of expression (index 0,2) are literal const or vars
# index 3 being array of three indexes:
#   first: value (const or var)
#   second: comparison (<, <=, >=, >, ==)

