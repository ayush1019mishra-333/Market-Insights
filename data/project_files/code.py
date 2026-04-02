import pandas as pd
import mysql.connector
import os
import numpy as np   # IMPORTANT

# File → table mapping
csv_files = [
    ('begin_inventory.csv', 'begin_inventory'),
    ('end_inventory.csv', 'end_inventory'),
    ('purchase_prices.csv', 'purchase_prices'),
    ('purchases.csv', 'purchases'),
    ('sales.csv', 'sales'),
    ('vendor_invoice.csv', 'vendor_invoice')
]

# Connect to MySQL
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Amit@1019',
    database='Market_Insights'
)

cursor = conn.cursor()

folder_path = r'C:\Users\amit6\OneDrive\Desktop\Market_Insights\data\data'


# Detect SQL type
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'BIGINT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'


for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)

    print(f"\n🚀 Processing {csv_file}...")

    # STEP 1: DROP old table
    cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    conn.commit()

    # STEP 2: Read sample
    sample_df = pd.read_csv(file_path, nrows=100)

    # Clean column names
    sample_df.columns = [
        col.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
        for col in sample_df.columns
    ]

    # STEP 3: Create table
    columns = ', '.join([
        f'`{col}` {get_sql_type(sample_df[col].dtype)}'
        for col in sample_df.columns
    ])

    create_table_query = f"""
    CREATE TABLE `{table_name}` (
        {columns}
    )
    """

    cursor.execute(create_table_query)
    conn.commit()

    total_rows = 0

    # STEP 4: Insert in chunks
    for chunk in pd.read_csv(file_path, chunksize=50000):

        chunk.columns = [
            col.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
            for col in chunk.columns
        ]

        numeric_cols = ['Volume', 'Price', 'Quantity', 'Dollars']

        for col in numeric_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

        # Replace NaN with None
        chunk = chunk.replace({np.nan: None})

        cols = ', '.join([f'`{col}`' for col in chunk.columns])
        placeholders = ', '.join(['%s'] * len(chunk.columns))

        insert_query = f"""
        INSERT INTO `{table_name}` ({cols})
        VALUES ({placeholders})
        """

        data = [tuple(row) for row in chunk.values]

        cursor.executemany(insert_query, data)
        conn.commit()

        total_rows += len(data)

        print(f"Inserted {len(data)} rows into {table_name}")

    print(f"✅ Total rows inserted into {table_name}: {total_rows}")

cursor.close()
conn.close()

print("\n✅ All CSV files loaded successfully!")