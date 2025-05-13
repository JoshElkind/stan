import pandas as pd

def shorten(s, e, path):
    df = pd.read_csv(path)
    df = df.iloc[s:e]
    df.to_csv(path, index=False) 

def num_rows_csv(path):
    df = pd.read_csv(path)
    print(len(df))

# shorten(0, 800000, "data/t1APPL.csv")

# num_rows_csv("data/t2APPL.csv")
