import pandas as pd 

# structure:

### FIRST
# outer-consts: array of [value_type, "var_name", value]
# value types: 
# 1. const (value_type = 0) - value will be a direct constant integer
# 2. reliant_const (1) - value will be an arr with:
# index 1 being another array containing 0 or 1 representing if second position values are const or variables,
# index 2 being the array containing const value themsleves or variable values
# index 3 being array combining method (+, -, *, /, ...) for values (length of this array is always one less than index 2 array)
# 3. row_calculated (2) - will be calculated with some kind of row operation on df, will be an array:
# 
