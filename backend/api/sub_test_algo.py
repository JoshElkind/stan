import pandas as pd

def get_asset_data(table_name, engine):
    query = f'SELECT * FROM "{table_name}"'
    return pd.read_sql(query, engine)
