import pandas as pd

def shorten(s, e, path):
    df = pd.read_csv(path)
    print(f"Original rows: {len(df)}")
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[["Date","Open","High","Low","Close","Volume"]]
    df = df.iloc[s:e]
    print(f"Trimmed rows: {len(df)}")
    df.to_csv(path, index=False) 

def num_rows_csv(path):
    df = pd.read_csv(path)
    print(len(df))

shorten(4000000, 4100000, "cAPPL.csv")

# num_rows_csv("data/t2APPL.csv")
