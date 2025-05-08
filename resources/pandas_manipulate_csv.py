import pandas as pd 
import matplotlib.pyplot as plt


dataframe = pd.read_csv("HON.csv") 
# loads the file HON.csv to be manipulated by the pandas library by loading 
# it into a dataframe

### VIEWING BASIC STUFF ###

first_5_rows = dataframe.head() 
### print(first_5_rows)
# .head() retrieves first 5 rows of a csv file (assuming it has at least 5 rows)

last_5_rows = dataframe.tail()
### print(last_5_rows)
# .tail() retrieves last 5 rows of a csv file (assuming it has at least 5 rows)

columns = dataframe.columns # array of strings representing column names
### print(columns)
### print(columns[0])
# this works because we are accessing the attribute of the dataframe object 'columns'
# which is why we get an error with .columns(), since we are getting the attribute
# not calling a method...

### BASIC CLEANING / MUTATION ###

dataframe.dropna()
# drops ANY row with missing values (at least one blank value)

dataframe["Volume"] *= 1000 
# scales up EACH element in the csv file dataframe by 1000

dataframe["Date"] = pd.to_datetime(dataframe["Date"])
# here we set the Date column to itself but converted to a date time 
# format using pandas library (date YEAR/MONTH/DAY + time 00:00:00)

# to change mth row index, nth column index:
### df.iloc[m, n] = VALUE
# OR: to change mth row index, COLUMN_NAME:
### df.iloc[n, df.columns.get_loc("COLUMN_NAME")] = VALUE

### FILTERING ###

### NOTE: when filtering, pandas will often NOT keep the original ordering of the rows
# so we must after filtering restore it with an operation like .sort_values(), seen below

new_dataframe1 = dataframe[dataframe['Volume'] > 1000] 
new_dataframe1 = new_dataframe1.sort_values('Date')
# makes a new dataframe where only the rows where the Volume is greater than 1000
# kept and copied over

new_dataframe2 = dataframe[(dataframe['High'] - dataframe['Low']) > 1] 
new_dataframe2 = new_dataframe2.sort_values('Date') 
# makes a new dataframe where only the rows where the price change between High
# and Low prices is greater than one (in the minute) are copied over

### AVERAGE OF COL ###

average_open_price = dataframe['Open'].mean() 
# finds the average Open price and stores it (as a float I assume)

### new_dataframe1.to_csv('cleaned_HON.csv', index=False)
# once edits are done, we can save this newly changed dataframe to a 
# new csv file
# NOTE: the index=False is needed since Pandas automatically adds a index column
# for us whenever we import into a dateframe (can see this by above printes) but
# usually we don't want to save this back to a new csv after we are done data 
# manipulation, so we must omit it with index=False

### dataframe.to_csv('HON.csv', index=False)
# NOTE: we can actually save to original file (AKA edit the file we are importing
# dataframe to) with simply referencing the name 

### print(new_dataframe1['Open'])
# here we try accesing the "Open" column of the new_dataframe1 dataframe
# but pandas will only let print a summary of first couple rows amd last couple
# along with some additional info
# in order to print all rows, we must loop through each row at a time

value_counts = dataframe['Open'].value_counts()
### print(value_counts)
# .value_counts() method goes through given column (in this case Open)
# and counts for each unique value how much they occur, also it provides some 
# additional info such as how many unique values there are (note: in a file as 
# large as this, the value_count will omit most of the unique values)
# also will be sorted, with more common values at top lines...

# ^^^
### The .value_counts() retruns a series, special type of data structure where
# we can access a certain fields count by writing it as an index to the series:
### print(value_counts[153.998])  # will print the amount of times 153.998 appears in the column

# ^^^
# or if we don't know the indexes we can loop through them for example with:

value_counts = dataframe['Open'].value_counts()
number_of_unique_elements = len(dataframe['Open'].unique())

# for i in range(number_of_unique_elements):
    # print("Item: " + str(value_counts.index[i]) + "  -  appears " + str(value_counts.iloc[i]) + " time(s).")
# from most frequent to least

# for i in range(number_of_unique_elements):
    # print("Item:" + str(value_counts.index[number_of_unique_elements - (i + 1)]) + "  -  " + str(value_counts.iloc[number_of_unique_elements - (i + 1)]) + " time(s).")
    # from least frequent to most

# print(dataframe.iloc[0]) # prints first row of csv file
# print(dataframe.iloc[0].iloc[0]) # prints first column of first row of csv file
# print(dataframe.index[0]) # just prints first valid (non NULL) element
### NOTE: 
# as we can see above in the prints and the for loops where we print the unique
# values and their corresponding frequency:
# .index[n] simply presents us the first n non-NULL valid index that appeared in
# dataframes, arrays, or any data structure
# .iloc[n] presents us with the ACTUAL VALUE at that n non-NULL valid index...

dataframe = dataframe.sort_values('Volume', ascending=False)
# as we can see here, we can also rearrange the rows of a dataframe with 
# sorting - here we sorted the dataframe/csv so that rows with highest Volume
# attribute lay at the top...
### dataframe.to_csv('volume_descending.csv', index=False)


### COLUMN MANIPULATION ###

close_column = dataframe['Close'] # gets a series of Close column values
number_of_rows = len(close_column) # can find number of rows this way
last_Close_value = close_column.iloc[number_of_rows - 1] # last value in this column
### print(last_Close_value)


# NOTE: or we can create a new data frame only with those columns
# this helps since we can essentially reorder the columns in any way we want
# IMPORTANT: we must surround in extra pair of [] to include multiple cols...
only_open_close_cols = dataframe[['Open', 'Close']]
# below we swap the cols:
swap_only_open_close_cols = only_open_close_cols[['Close','Open']]
### swap_only_open_close_cols.to_csv('swap_only_open_close_cols.csv', index=False)


### CREATING NEW COLUMN ###
# lets add a new column to find the difference (absolute) between the Close and Open 
swap_only_open_close_cols['Diff'] = abs(swap_only_open_close_cols['Close'] - swap_only_open_close_cols['Open'])

swap_only_open_close_cols.rename(columns={'Diff': 'Change'}, inplace=True) # RENAMING COLUMN: Diff to Change

# we can also, for example, move the Change Col (renamed) to the front with:
array_of_col_names = ['Change']
for col in swap_only_open_close_cols.columns:
    if col != 'Change':
        array_of_col_names.append(col)
swap_only_open_close_cols = swap_only_open_close_cols[array_of_col_names]


### Extracting Parts of Datetime ###
series_of_dates = dataframe['Date'].dt.date       # (e.g., 2022-01-01)
series_of_hours = dataframe['Date'].dt.hour         # (e.g., 14)
series_of_days = dataframe['Date'].dt.dayofweek    # 0 = Monday, 6 = Sunday
### NOTE: will be a series just like any other singular column selection


### WORKING / FILTERING DATES ###

# here we only select minute data belonging to date of Dec 1, 2021
select_certain_day_only = dataframe[dataframe['Date'].dt.date == pd.to_datetime('2021-12-01').date()]
select_certain_day_only = select_certain_day_only.sort_values('Date')
### select_certain_day_only.to_csv('oneday.csv', index=False)

# here we can combine multiply boolean conditions for picking rows, and we specify and time range
### THIS WONT WORK, MUST USE PANDAS BITWISE OPERATOR (&): select_time_frame = dataframe[dataframe['Date'] >= '2022-01-01' and dataframe['Date'] <= '2022-01-03']
select_time_frame = dataframe[(dataframe['Date'].dt.date >= pd.to_datetime('2022-01-04').date()) & 
                              (dataframe['Date'].dt.date <= pd.to_datetime('2022-01-05').date())]
select_time_frame = select_time_frame.sort_values('Date')
### select_time_frame.to_csv('timerange.csv', index=False)


### GROUPING AND AGGREGATING ###
# grouping basically combines rows of your dataframe by common values of a specified column
# ex: we can group together rows that share same date
# now aggregating means what do we do with these new groups
# ex: when combining multiple we want to combine their volumes to reflect
# total volume per day:
grouped = dataframe.groupby('Date').agg({'Close': 'mean', 'Volume': 'sum'}) 
### grouped.to_csv('grouped.csv', index=False)
# NOTE: # will only include columns we specify, thus since we speicifed Close and Volume only
# all the rest will be ommited, also note that since each Date column item is unique we essentially grouped 
# nothing and this operation is the same as simply selecting the Close and Volume columns of grouped

# Common Aggregate Functions: 
# 'mean' – average
# 'sum' - total
# 'max', 'min', 'count', etc.

# ^^^
### If we want to properly grab the minute data and sort of convert it to daily data ourselves we need
# a couple things such as: 1. Custom aggregate functions to select first and last group price points
# and also special identifier to identify same days
def first_price(series):
    return series.iloc[0]
def last_price(series):
    length = len(series)
    return series.iloc[length - 1]
def datetime_to_date(series):
    return series
daily = dataframe.groupby(dataframe['Date'].dt.date).agg({'Open': first_price, 
                                                          'High': 'max', 
                                                          'Low': 'min', 
                                                          'Close': last_price, 
                                                          'Volume': 'sum'}) 
# Notice the new Columns come out in order we apply .agg() to original columns
# We also made it so that the grouping only looks at the date portion of Date 
# column when assesing uniqueness

### daily.to_csv('daily_converted.csv', index=False) 

# BUT NOTICE: when we group by a certain column or metric, that column values are not
# included in the final grouping csv (unless we include .reset_index() at very end):
dataframe = dataframe.sort_values('Date') 
daily_with_date = dataframe.groupby(dataframe['Date'].dt.date).agg({'Open': first_price, 
                                                                    'High': 'max', 
                                                                    'Low': 'min', 
                                                                    'Close': last_price, 
                                                                    'Volume': 'sum'}).reset_index()
### daily_with_date.to_csv('daily_converted_with_date.csv', index=False) 

### DELETING ROWS ###

dataframe_small = pd.read_csv("HON_SMALL.csv") 

dataframe_small = dataframe_small.drop(index=(len(dataframe_small) - 2))
# here we drop the second last row, by subtracting one from last index
# len(dataframe) - 1, would actually be the last row...

# NOW LETS ADD A ROW WHERE WE PREVIOUSLY DELETED:
new_row1 = pd.DataFrame([{
    'Date':1,
    'Open':1,
    'High':1,   # new_row1 is technically a singular sized dataframe (one row)
    'Low':1,
    'Close':1,
    'Volume':1
}])
# ADDING:
dataframe_small = pd.concat([dataframe_small, new_row1], ignore_index=True)
# basically we added the new_row at the bottom of the previous dataframe

### WHAT IF WE WANT TO ADD AT THE N-th INDEX???
# well we do:
new_row2 = pd.DataFrame([{
    'Date':7,
    'Open':7,
    'High':7,   # new_row2 is technically a singular sized dataframe (one row)
    'Low':7,
    'Close':7,
    'Volume':7
}])
n_th_row_index_to_insert_at = 3 # can change to any index we want to insert at...
dataframe_small = pd.concat([dataframe_small.iloc[:n_th_row_index_to_insert_at], 
                             new_row2, dataframe_small.iloc[n_th_row_index_to_insert_at:]], ignore_index=True)

dataframe_small.to_csv('small_deleted.csv', index=False) 

### MAKING OWN DATAFRAMES (not importing csv files) ###
# we can do this by same train of though with the new_rows above
# simply:

new_dataframe_selfmade = pd.DataFrame([
    {'Cat':1,
    'Name':'moo'},
    {'Cat':2,
    'Name':'fluff'},    # This creates our own custom csv dataframe, with columns
    {'Cat':3,           # columns named Cat and Name...
    'Name':'cooki'}
])
### new_dataframe_selfmade.to_csv('selfmade.csv', index=False)

### PLOTTING (BASIC) ###

plt.figure(figsize=(12, 6))
plt.plot(daily_with_date['Date'], daily_with_date['Close'], label='Close Price', color='blue', linewidth=1.5)
plt.title("Close Price Over Time")
plt.xlabel("Date")
plt.ylabel("Price")
plt.grid(True)
plt.legend()
plt.tight_layout()
### plt.show()
