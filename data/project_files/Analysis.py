import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt 
import seaborn as sns 
import numpy as np
import warnings
from scipy.stats import ttest_ind
import scipy.stats as stats
warnings.filterwarnings('ignore')

db1 = mysql.connector.connect( host = "localhost",
                              user = "root",
                              password = "Amit@1019",
                              database = "Market_Insights")

cur = db1.cursor()
def get_required_data(cur):
    query = "SELECT * FROM vendor_sales_summary"
    
    cur.execute(query)
    data = cur.fetchall()
    
    columns = [col[0] for col in cur.description]  # get column names
    
    df = pd.DataFrame(data, columns=columns)
    
    print("\nActual Required Table\n")
    print(df)
   

# Exploratory Data Analysis
# ============================
# Previously, we examined the various tables in the database to identify key variables,
# understand their relationships, and determine which ones should be included in the final analysis.
# In this phase of EDA, we will analyze the resultant table to gain insights into the distribution of each column.
# This will help us understand data patterns, identify anomalies, and ensure data quality before proceeding with further analysis.


# Summary Statistics
    print(df.describe().T)
    
    # Distribution Plots for Numerical Columns
    numerical_cols = df.select_dtypes(include=np.number).columns

    plt.figure(figsize=(12, 8))

    for i, col in enumerate(numerical_cols):
     plt.subplot(5, 4, i + 1)  
     sns.histplot(df[col], kde=True, bins=30)
    plt.title(col)

    plt.tight_layout()
    plt.show()
    
    
    # Outlier detection with boxplot
    
    plt.figure(figsize=(12, 8))

    for i, col in enumerate(numerical_cols):
     plt.subplot(5, 4, i + 1)  
     sns.boxplot(y = df[col])
     plt.title(col)

    plt.tight_layout()
    plt.show()


# =======================================
# SUMMARY STATISTICS INSIGHTS :
# =======================================

# Negative & Zero Values:
# Gross Profit: Minimum value is -52,002.78, indicating losses. Some products or
# transactions may be selling at a loss due to high costs or selling at discounts lower than the purchase price.

# Profit Margin: Has a minimum of -∞, which suggests cases where revenue is zero or even lower than costs.
# Total Sales Quantity & Sales Dollars: Minimum values are 0, meaning some products were purchased but never sold.
# These could be slow-moving or obsolete stock.

# Outliers Indicated by High Standard Deviations:
# Purchase & Actual Prices: The max values (5,681.81 & 7,499.99) are significantly higher than the mean (24.39 & 36.64),
# indicating potential premium products.

# Freight Cost: Huge variation, from 0.09 to 257,032.07, suggests logistics inefficiencies or bulk shipments.

# Stock Turnover: Ranges from 0 to 274.5, implying some products sell extremely fast while others remain in stock indefinitely.
# Value more than 1 indicates that sold quantity for that product is higher than purchased quantity due to either sales are being
# fulfilled from older stock.


# Going for the new dataset where we will remove some outliers 

def get_new_dataset(cur):
        
    query ="""Select * from vendor_sales_summary where 
    GrossProfit > 0 
    and ProfitMargin > 0
    and TotalSalesQuantity > 0 ;"""
    cur.execute(query)
    data = cur.fetchall()
    
    columns = [col[0] for col in cur.description]  # get column names
    
    df = pd.DataFrame(data, columns=columns)
    
    print("\nNew Table without outliers\n")
    print(df)    


    # Distribution Plots for New Numerical Columns
    numerical_cols = df.select_dtypes(include=np.number).columns

    plt.figure(figsize=(12, 8))

    for i, col in enumerate(numerical_cols):
        plt.subplot(5, 4, i + 1)  
        sns.histplot(df[col], kde=True, bins=30)
        plt.title(col)

    plt.tight_layout()
    plt.show()
    
    plt.figure(figsize=(12, 6))
# Vendor count plot
    plt.subplot(1, 2, 1)
    sns.countplot(
    y=df["VendorName"],
    order=df["VendorName"].value_counts().head(10).index)
    plt.title("Top 10 Vendors")

# Description plot using sales quantity
    top_desc = df.sort_values(
    by="TotalSalesQuantity",
    ascending=False).head(10)

    plt.subplot(1, 2, 2)
    sns.barplot(
    x=top_desc["TotalSalesQuantity"],
    y=top_desc["Description"])
    plt.title("Top 10 Products by Sales")

    plt.tight_layout()
    plt.show()
    
    # Correlation Heat-Map
    
    plt.figure(figsize=(12,8))
    correlation_matrix = df[numerical_cols].corr()
    sns.heatmap(correlation_matrix , annot = True , fmt = ".2f" , cmap = "coolwarm" , linewidths= 0.5)
    plt.title("Correlation HeatMap")
    plt.show()

# =====================================
# DATA ANALYSIS AS PER THE DATA
# =====================================

def get_brand_performance(cur):
    
    query = "SELECT * FROM vendor_sales_summary"
    
    cur.execute(query)
    data = cur.fetchall()

    df = pd.read_sql(query, db1)
    Brand_performance = df.groupby('Description').agg({
    'TotalSalesDollars' : 'sum',
    'ProfitMargin' : 'mean'
}).reset_index()

    print("\nThis is the brand performance from the required table\n")
    print(Brand_performance)
    
    low_sales_threshold = Brand_performance['TotalSalesDollars'].quantile(0.20)
    High_margin_threshold = Brand_performance['ProfitMargin'].quantile(0.80)
    
    print(low_sales_threshold)
    print(High_margin_threshold)
   
   
   
   #Filtering out with low sales and high profit margin 
    target_brands = Brand_performance[
        (Brand_performance['TotalSalesDollars'] <= low_sales_threshold) &
        (Brand_performance['ProfitMargin'] >= High_margin_threshold)
    ]
    print("Brands with Low Sales and High Profit Margin :")
    print(target_brands.sort_values('TotalSalesDollars'))


# Better for Visualization
    Brand_performance = Brand_performance[Brand_performance['TotalSalesDollars']<10000]
# Scatter Plot 
    plt.figure(figsize=(10,6))
    sns.scatterplot(
    data=Brand_performance,
    x='TotalSalesDollars',
    y='ProfitMargin',
    color='blue',
    alpha=0.1,
    s=10,
    label='All Brands'
)

# Target brands (highlighted)
    sns.scatterplot(
    data=target_brands,
    x='TotalSalesDollars',
    y='ProfitMargin',
    color='red',
    s=40,
    label='Target Brands'
)

# Correct threshold lines
    plt.axhline(High_margin_threshold,
    linestyle='--',    
    color='black',
    label='High Margin Threshold'
)

    plt.axvline(
    low_sales_threshold,
    linestyle='--',
    color='black',
    label='Low Sales Threshold'
)

# Fix axis scaling
    plt.ylim(0, 100)

    plt.xlabel("Total Sales ($)")
    plt.ylabel("Profit Margin (%)")
    plt.title("Brands for Promotional or Pricing Adjustments")

    plt.legend()
    plt.grid(True)

    plt.show()


# Function to represent ka quantity or value in better manner
    def format_dollars(value):
        if value >= 1_000_000:
            return f"{value / 1_000_000: .2f}M"
        elif value >= 1_000:
            return f"{value / 1_000:2f}K"
        else:
            return str(value)
        
# ===============================================
# Which Vendors and brands demonstrate the highest sales performance ?
# ===============================================

    top_vendors = df.groupby("VendorName")["TotalSalesDollars"].sum().nlargest(10)
    top_brands = df.groupby("Description")["TotalSalesDollars"].sum().nlargest(10)
    print(top_vendors.apply(lambda x : format_dollars(x)))
    print("\n")
    print(top_brands.apply(lambda x : format_dollars(x)))
    print("\n")
    
    plt.figure(figsize=(15,5))
# Plot for Top Vendors
    plt.subplot(1, 2, 1)
    ax1 = sns.barplot(y=top_vendors.index, x=top_vendors.values, palette="Blues_r")
    plt.title("Top 10 Vendors by Sales")

    for bar in ax1.patches:
     ax1.text(
        bar.get_width() + (bar.get_width() * 0.02),
        bar.get_y() + bar.get_height() / 2,
        format_dollars(bar.get_width()),
        ha='left',
        va='center',
        fontsize=10,
        color='black'
    )

# Plot for Top Brands
    plt.subplot(1, 2, 2)
    ax2 = sns.barplot(y=top_brands.index.astype(str), x=top_brands.values, palette="Reds_r")
    plt.title("Top 10 Brands by Sales")

    for bar in ax2.patches:
     ax2.text(
        bar.get_width() + (bar.get_width() * 0.02),
        bar.get_y() + bar.get_height() / 2,
        format_dollars(bar.get_width()),
        ha='left',
        va='center',
        fontsize=10,
        color='black'
    )

    plt.tight_layout()
    plt.show()



# ===============================================
# Which Vendors contribute the most to total purchase dollars ?
# ===============================================


    print("Vendors Contribution to Total Purchase Dollars")
    Vendors_contribution = df.groupby('VendorName').agg({
    'TotalPurchaseDollars' : 'sum',
    'GrossProfit' : 'sum',
    'TotalSalesDollars' : 'sum'
}).reset_index()
    
    print(Vendors_contribution)
    print("\n")
    
    Vendors_contribution['PurchaseContribution%'] = Vendors_contribution['TotalPurchaseDollars'] /Vendors_contribution['TotalPurchaseDollars'].sum()*100
    
    Vendors_contribution = Vendors_contribution.sort_values('PurchaseContribution%' , ascending= False) 
    
    # Display Top 10 Vendors
    top_vendors = Vendors_contribution.head(10)
    top_vendors['TotalSalesDollars'] = top_vendors['TotalSalesDollars'].apply(format_dollars)
    top_vendors['TotalPurchaseDollars'] = top_vendors['TotalPurchaseDollars'].apply(format_dollars)
    top_vendors['GrossProfit'] = top_vendors['GrossProfit'].apply(format_dollars)
    top_vendors['Cumulative_Contribution%'] = (top_vendors['PurchaseContribution%'].cumsum().map('{:.2f}%'.format))
    print(top_vendors)
    print("\n")
    
    print(top_vendors['PurchaseContribution%'].sum())
    
    # Pareto Chart for the observations
    fig, ax1 = plt.subplots(figsize=(10, 6))

# Bar plot for Purchase Contribution %
    sns.barplot(x=top_vendors['VendorName'],y=top_vendors['PurchaseContribution%'],
    palette="mako", ax=ax1)

# Add labels on bars
    for i, value in enumerate(top_vendors['PurchaseContribution%']):
        ax1.text(i, value - 2, f"{value:.1f}%", ha='center', va = 'top',
        fontsize=9,
        color='white'
    )

# Line plot for Cumulative Contribution %
    ax2 = ax1.twinx()
    ax2.plot( top_vendors['VendorName'], top_vendors['Cumulative_Contribution%'], color='red',
    marker='o',
    linestyle='dashed',
    label='Cumulative Contribution'
)

# Axis labels and title
    ax1.set_xlabel('Vendors')
    ax1.set_ylabel('Purchase Contribution %', color='blue')
    ax2.set_ylabel('Cumulative Contribution %', color='red')
    ax1.set_title('Pareto Chart: Vendor Contribution to Total Purchases')

# Rotate x-axis labels
    ax1.set_xticklabels(top_vendors['VendorName'], rotation=90)

# 100% reference line
    ax2.axhline(y=100, color='gray', linestyle='dashed', alpha=0.7)

# Legend
    ax2.legend(loc='upper right')

# Layout fix
    plt.tight_layout()
    plt.show()
    
    
# How much a total procurement is dependent on the top vendors ?    

    print(f"Total Purchase Contribution of Top 10 vendors is {round(top_vendors['PurchaseContribution%'].sum(),2)}%")
    
    # Representing the contribution with the help of Donut chart 
    vendors = list(top_vendors['VendorName'].values)
    purchase_contributions = list(top_vendors['PurchaseContribution%'].values)
    total_contribution = sum(purchase_contributions)
    remaining_contributions = 100 - total_contribution
    
    # Append Other vendors category
    vendors.append("Other Vendors")
    purchase_contributions.append(remaining_contributions)
    
    # Donut Chart
    fig, ax = plt.subplots(figsize =(8,8))
    wedges , texts , autotexts = ax.pie(purchase_contributions, labels = vendors , autopct = '%1.1f%%',
                                        startangle=140 , pctdistance= 0.85,  colors = plt.cm.Paired.colors)
    
    # Draw a white circle in the centre to create a "donut" effect 
    centre_circle = plt.Circle((0,0) , 0.70 , fc='white')
    fig.gca().add_artist(centre_circle)
    
    # Add total contribution annotation in the centre 
    plt.text(0,0,f"Top 10 Total:\n{total_contribution:.2f}%", fontsize = 14 , 
             fontweight ='bold' , ha = 'center', va='center')
    
    plt.title("Top 10 vendors Purchase Contribution (%)")
    plt.show()


# Does purchasing in bulk reduce the unit price , and what is the optimal purchase volume for cost savings ?

    df['UnitPurchasePrice'] = np.where(
    df['TotalSalesQuantity'] != 0,
    df['TotalPurchaseDollars'] / df['TotalPurchaseQuantity'],0)
    df['OrderSize'] = pd.qcut(df['TotalPurchaseQuantity'], q = 3, labels =["Small" , "Medium" ,"Large"])
    Size_dataset = df.groupby('OrderSize')[['UnitPurchasePrice']].mean()
    print(Size_dataset)
    
    # Box plot for the above dataset
    
    plt.figure(figsize=(10,6))
    sns.boxplot(data = df , x = "OrderSize" , y ="UnitPurchasePrice" , palette = "Set2")
    plt.title("Impact of Bulk Purchasing on unit Price")
    plt.xlabel("Order size")
    plt.ylabel("Average Unit Purchase Price")
    plt.show()
    
    # 1) Vendors buying in bulk (Large Order Size) gets the lowest unit price ($11.29 per unit) , meaning higher margins if they can manage
    # inventory efficiency.
    # 2) The price difference between Small and Large orders is substaintial (~72% reduction in unit cost)
    # 3) This suggests that the bulk pricing successfully encourage vendors to purchase in larger volumes , leading to higher overall sales deposit 
    # lower per-unit revenue.
    
    
    # Which vendor have lowest turnover , indicating excess stock and slow-moving products?
    low_turnover = df[df['StockTurnover'] < 1].groupby('VendorName')[['StockTurnover']].mean().sort_values('StockTurnover', ascending=True).head(10)
    print(low_turnover)
    
    # How much capital is locked in unsold inventory per vendor , and which vendors contribute the most to it?
    total_capital = df["UnsoldInventoryValue"] = (df["TotalPurchaseQuantity"] - df["TotalSalesQuantity"]) * df["PurchasePrice"]
    print('Total Unsold Capital:' , format_dollars(total_capital.sum())) 
    
    df.to_csv("vendor_sales_summary",index = False)
    print("done")



# call function ONCE
get_required_data(cur)
get_new_dataset(cur)
get_brand_performance(cur)




cur.close()
db1.close()