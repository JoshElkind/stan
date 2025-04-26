import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker

URL_DATABASE = 'postgresql://postgres@localhost:5432/stock-data'
engine = create_engine(URL_DATABASE)
SessionLocal = sessionmaker(bind=engine)
metadata = MetaData()

def load_csv_to_db(csv_path: str, ticker: str):
    # Explicitly specify the columns you want to include
    usecols = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    
    dtype_dict = {col: float for col in usecols[1:]}  # All except 'Date' as float
    dtype_dict['Date'] = str  # 'Date' column should be treated as string to parse later
    
    df = pd.read_csv(csv_path, usecols=usecols, dtype=dtype_dict)
    
    # Rename columns to match the expected database format
    df.rename(columns={
        'Date': 'datetime',
        'Close': 'Adj Close',  # Rename Close to Adj Close for consistency with the original format
        'Low': 'Low Close'
    }, inplace=True)

    # Debugging: print the columns of the DataFrame to verify
    print("DataFrame columns:", df.columns)
    
    # Convert datetime column to the proper format
    df['datetime'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Drop any rows with missing values
    df.dropna(inplace=True)
    
    # First, check if the table exists and drop it to recreate with the correct schema
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
            # Prepare data for bulk insert
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append({
                    'datetime': row['datetime'],
                    'Open': row['Open'],
                    'High': row['High'],
                    'Low': row['Low Close'],  # 'Low' column in the CSV
                    'Close': row['Adj Close'],  # 'Adj Close' column in the CSV
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
    csv_file_path = "./data/appl1min.csv"
    ticker = "1minS-APPL"
    load_csv_to_db(csv_file_path, ticker)
