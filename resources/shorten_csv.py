import pandas as pd

def shorten(s, e, path):
    df = pd.read_csv(path)
    print(f"Original rows: {len(df)}")
    df['date'] = pd.to_datetime(df['date'])
    df = df[["date","open","high","low","close","volume"]]
    df = df.iloc[s:e]
    print(f"Trimmed rows: {len(df)}")
    df.to_csv(path, index=False) 

def num_rows_csv(path):
    df = pd.read_csv(path)
    print(len(df))

shorten(0, 5000, "tam.csv")

# num_rows_csv("data/t2APPL.csv")
