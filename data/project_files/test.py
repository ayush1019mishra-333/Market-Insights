import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt 
import seaborn as sns 
import numpy as np

# ==========================
# THESE ALL ARE THE IMPORTS ABOVE AND CONNECTION BELOW
# ==========================

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Amit@1019",
    database="Market_Insights"
)

# ==========================================
# The data is being loaded or got by using the below query
# ==========================================

query = """WITH FreightSummary AS (
    SELECT
        VendorNumber,
        SUM(Freight) AS FreightCost
    FROM vendor_invoice
    GROUP BY VendorNumber
),

PurchaseSummary AS (
    SELECT
        VendorNumber,
        VendorName,
        Brand,
        Description,
        AVG(PurchasePrice) AS PurchasePrice,
        SUM(Quantity) AS TotalPurchaseQuantity,
        SUM(Dollars) AS TotalPurchaseDollars
    FROM purchases
    WHERE PurchasePrice > 0
    GROUP BY
        VendorNumber,
        VendorName,
        Brand,
        Description
),

PriceSummary AS (
    SELECT
        Brand,
        MAX(Price) AS ActualPrice,
        MAX(Volume) AS Volume
    FROM purchase_prices
    GROUP BY Brand
),

SalesSummary AS (
    SELECT
        VendorNo,
        Brand,
        SUM(SalesQuantity) AS TotalSalesQuantity,
        SUM(SalesDollars) AS TotalSalesDollars,
        SUM(SalesPrice) AS TotalSalesPrice,
        SUM(ExciseTax) AS TotalExciseTax
    FROM sales
    GROUP BY VendorNo, Brand
)

SELECT
    ps.VendorNumber,
    ps.VendorName,
    ps.Brand,
    ps.Description,
    ps.PurchasePrice,
    pr.ActualPrice,
    pr.Volume,
    ps.TotalPurchaseQuantity,
    ps.TotalPurchaseDollars,
    
    COALESCE(ss.TotalSalesQuantity, 0) AS TotalSalesQuantity,
    COALESCE(ss.TotalSalesDollars, 0) AS TotalSalesDollars,
    COALESCE(ss.TotalSalesPrice, 0) AS TotalSalesPrice,
    COALESCE(ss.TotalExciseTax, 0) AS TotalExciseTax,
    COALESCE(fs.FreightCost, 0) AS FreightCost

FROM PurchaseSummary ps

LEFT JOIN PriceSummary pr
    ON ps.Brand = pr.Brand

LEFT JOIN SalesSummary ss
    ON ps.VendorNumber = ss.VendorNo
   AND ps.Brand = ss.Brand

LEFT JOIN FreightSummary fs
    ON ps.VendorNumber = fs.VendorNumber

ORDER BY ps.TotalPurchaseDollars DESC;
"""

# ==============================
# Below is the data read and cleaning part 
# ==============================

df = pd.read_sql_query(query, conn)

# These were the techniques to change the datatype and fill missing values and to manage the empty spaces along 

df['Volume'] = df['Volume'].astype('float64')
df['VendorName'] = df["VendorName"].str.strip()
print("Done")
df.fillna(0, inplace=True)
print(df.isnull().sum())


# New columns which are required for the analysis are created below 
numeric_cols = [
    'PurchasePrice',
    'ActualPrice',
    'TotalSalesQuantity',
    'TotalSalesDollars',
    'TotalPurchaseDollars'
]

for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

df.fillna(0, inplace=True)
df['UnitProfit'] = df['ActualPrice'] - df['PurchasePrice']

df['GrossProfit'] = (
    df['UnitProfit'] * df['TotalSalesQuantity']
)

df['ProfitMargin'] = np.where(
    df['ActualPrice'] != 0,
    (df['UnitProfit'] / df['ActualPrice']) * 100,
    0
)

df['StockTurnover'] = np.where(
    df['TotalPurchaseQuantity'] != 0,
    df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'],
    0
)

df['SalesToPurchaseRatio'] = np.where(
    df['TotalPurchaseDollars'] != 0,
    df['TotalSalesDollars'] / df['TotalPurchaseDollars'],
    0
)
print("done")

# To load the data in the newly created table including all the columns old + new 
print("\nLoading the complete data in the required table ....\n")

df.replace([np.inf, -np.inf], np.nan, inplace=True)
df.fillna(0, inplace=True)

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:Amit%401019@localhost/Market_Insights")

df.to_sql('vendor_sales_summary', con=engine, if_exists='replace', index=False)
print("Task Completed")

query = "SELECT * FROM vendor_sales_summary "
df = pd.read_sql(query, con=engine)


print(df)
print("\n")
print("Finally Done")

