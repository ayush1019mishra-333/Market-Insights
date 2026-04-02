# =================
# All the Imports 
# =================

import pandas as pd
import matplotlib.pyplot as plt 
import seaborn as sns 
import mysql.connector
import numpy as np

# ===================
# Connections 
# ===================

db1 = mysql.connector.connect( host = "localhost",
                              user = "root",
                              password = "Amit@1019",
                              database = "Market_Insights")

cur = db1.cursor()

# ============================
# SUMMARY OF ALL THE QUERIES
# ============================

# The data required for the summary table are joined together in a single table which will be used for analysis :
# 1. purchase transactions made by vendors
# 2. sales transaction data
# 3. freight cost for each vendor
# 4. acutal product prices from vendors 

# ============================
# 1. Total FreightCost Summary 
# ============================

def get_frightcost(cur):
    query = """SELECT vendorNumber , round(sum(freight),2) as total_cost 
               from vendor_invoice 
               group by vendorNumber
               order by vendorNumber;"""
               
               
    cur.execute(query)
    data = cur.fetchall()

    print("\nFreight Cost Summary\n")
    
    df = pd.DataFrame(data, columns=['VendorNumber' , 'Freight_cost' ])
    print(df)
               
               
               
# ============================
# 2. Total Price Summary 
# ============================    
               
def get_totalprice(cur):
    query = """SELECT 
    p.VendorNumber,
    UPPER(p.VendorName) AS VendorName,
    p.Brand,
    p.Description,
    p.PurchasePrice,
    pp.Volume,
    pp.Price AS ActualPrice,
    p.Quantity,
    p.PurchasePrice * p.Quantity AS TotalPurchaseDollars
FROM purchases p
JOIN purchase_prices pp
    ON p.Brand = pp.Brand
    AND p.Size = CONCAT(pp.Volume, 'mL')
    limit 100"""
    
    cur.execute(query)
    data = cur.fetchall()

    print("\nTotal Price Summary\n")
    
    df = pd.DataFrame(data, columns=['VendorNumber' , 'VendorName' , 'Brand' ,'Description', 'PurchasePrice' , 'Volume' , 'ActualPrice' , 'TotalQuantity' ,'TotalDollars' ])
    print(df)
               
               
               
# ============================
# 2. Actual Net Summary Table
# ============================ 
               
            
def get_totaltable(cur):
    query="""WITH CleanPurchases AS (
    SELECT DISTINCT
        VendorNumber,
        VendorName,
        Brand,
        Description,
        PurchasePrice,
        Quantity,
        Dollars
    FROM purchases
    WHERE PurchasePrice > 0
),

FreightSummary AS (
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
    FROM CleanPurchases
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
ORDER BY ps.TotalPurchaseDollars DESC;"""   


    cur.execute(query)
    data = cur.fetchall()    
    
    print("\nVendors-Sales Summary Table\n")
    
    df = pd.DataFrame(data,columns=['VendorNumber' ,'VendorName' , 'Brand' , 'Description'  , 'PurchasePrice' , 'ActualPrice' , 'Volume' , 'TotalPurchaseQuantity', 'TotalpurchaseDollars' , 'TotalsalesQuantity' ,'TotalsalesDollars' , 'TotalsalesPrice','TotalExciseTax' , 
                                    'TotalFreightCost'])
    # print(df)
    
def get_value(cur):
    query = """SELECT SUM(TotalPurchaseDollars)
FROM (
    SELECT
        VendorNumber,
        Brand,
        Description,
        SUM(Dollars) AS TotalPurchaseDollars
    FROM purchases
    WHERE PurchasePrice > 0
    GROUP BY VendorNumber, Brand, Description
) x;"""
    cur.execute(query)
    data = cur.fetchall()
    
    
    df = pd.DataFrame(data)
    print(df)    

 

# ==========================              
# Performance Optimization for the above query :
# ==========================

# The query involves heavy joins and aggregations on large datasets like sales and purchases.
# Storing in pre-aggregated results avoids repeated expensive computations.
# Helps in analyzing sales, purchases, and pricing for different vendors and brands.
# Future Benefits of Storing this data for faster Dashboarding & Reporting.
# Instead of running expensive queries each time, dashboards can fetch data quickly from vendor_sales_summary.               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
               
# ============================
# EXECUTION PART OF QUERY
# ============================

get_frightcost(cur)
get_totalprice(cur)
get_totaltable(cur)
get_value(cur)














# ============================
# CLOSING PART OF QUERIES
# ============================

cur.close()
db1.close()