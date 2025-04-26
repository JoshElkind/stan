import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker

URL_DATABASE = 'postgresql://postgres@localhost:5432/stock-data'
engine = create_engine(URL_DATABASE)
SessionLocal = sessionmaker(bind=engine)
metadata = MetaData()

def load_csv_to_db(csv_path: str, ticker: str):
    # Explicitly specify the columns you want to include
    usecols = ['Open', 'High', 'Low', 'Close', 'Volume', 'datetime']
    
    dtype_dict = {col: float for col in usecols[:-1]}  # All except datetime as float
    dtype_dict['datetime'] = str
    
    df = pd.read_csv(csv_path, usecols=usecols, dtype=dtype_dict)
    df.dropna(inplace=True)
    
    # Debugging: print the columns of the DataFrame to verify
    print("DataFrame columns:", df.columns)
    
    # First, check if the table exists and drop it to recreate with correct schema
    inspector = inspect(engine)
    if ticker in inspector.get_table_names():
        # If table exists, drop it
        print(f"Table '{ticker}' already exists. Dropping it to recreate.")
        metadata_temp = MetaData()
        old_table = Table(ticker, metadata_temp, autoload_with=engine)
        old_table.drop(engine)
    
    # Create table with proper schema
    stock_table = Table(
        ticker, metadata,
        Column('datetime', String, primary_key=True),
        Column('Open', Float),
        Column('High', Float),
        Column('Low', Float),
        Column('Close', Float),
        Column('Volume', Float),
    )
    
    # Explicitly create table
    stock_table.create(engine, checkfirst=True)
    print(f"Table '{ticker}' created with columns: {[c.name for c in stock_table.columns]}")
    
    try:
        # Use connection for bulk insert
        with engine.connect() as connection:
            # Use bulk insert for better performance
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append({
                    'datetime': row['datetime'],
                    'Open': row['Open'],
                    'High': row['High'],
                    'Low': row['Low'],
                    'Close': row['Close'],
                    'Volume': row['Volume'],
                })
            
            if data_to_insert:
                connection.execute(stock_table.insert(), data_to_insert)
                connection.commit()
                print(f"Inserted {len(data_to_insert)} rows into table '{ticker}'")
            
            print(f"Data successfully loaded into table '{ticker}'.")
    except Exception as e:
        print("Error writing to database:", e)

if __name__ == "__main__":
    csv_file_path = "/.csv"
    ticker = "1minS__"
    load_csv_to_db(csv_file_path, ticker)