from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

date_str = sys.argv[1]

spark = SparkSession.builder.master("yarn").appName("demo").getOrCreate()

df_sales = spark.read.option("header","true").option("inferSchema","True").option("delimiter",",").csv(f"s3://midterm-data-dump-ag/sales_{date_str}.csv")
df_store = spark.read.option("header","true").option("inferSchema","True").option("delimiter",",").csv(f"s3://midterm-data-dump-ag/store_{date_str}.csv")
df_inventory = spark.read.option("header","true").option("inferSchema","True").option("delimiter",",").csv(f"s3://midterm-data-dump-ag/inventory_{date_str}.csv")
df_product = spark.read.option("header","true").option("inferSchema","True").option("delimiter",",").csv(f"s3://midterm-data-dump-ag/product_{date_str}.csv")
df_calendar = spark.read.option("header","true").option("inferSchema","True").option("delimiter",",").csv(f"s3://midterm-data-dump-ag/calendar_{date_str}.csv")

# Add a new column to df_calendar that indicates the end day of the week
df_calendar = df_calendar.withColumn("END_DAY_OF_WK", date_add(next_day("CAL_DT", "Sun"), -1))

# Join all dataframes
df_merged = df_sales.join(df_calendar, df_sales.TRANS_DT == df_calendar.CAL_DT, 'inner')\
                    .join(df_inventory, 
                          (df_sales.PROD_KEY == df_inventory.PROD_KEY) & 
                          (df_sales.STORE_KEY == df_inventory.STORE_KEY) & 
                          (df_sales.TRANS_DT == df_inventory.CAL_DT), 
                          'inner')

# Select the required columns
df_merged = df_merged.select(
    df_sales.PROD_KEY,
    df_sales.STORE_KEY,
    df_sales.TRANS_DT,
    df_sales.SALES_QTY,
    df_sales.SALES_PRICE,
    df_sales.SALES_AMT,
    df_sales.SALES_COST,
    df_calendar.DAY_OF_WK_NUM,
    df_calendar.END_DAY_OF_WK,
    df_inventory.INVENTORY_ON_HAND_QTY,
    df_inventory.INVENTORY_ON_ORDER_QTY,
    df_inventory.OUT_OF_STOCK_FLG
)

# Add a new column 'Low_Stock_flg'
df_merged = df_merged.withColumn('Low_Stock_flg', when(df_merged.INVENTORY_ON_HAND_QTY < df_merged.SALES_QTY, 1).otherwise(0))

# Perform groupBy and aggregations
df_total = df_merged.groupBy('END_DAY_OF_WK', 'PROD_KEY', 'STORE_KEY')\
                      .agg(round(sum('SALES_QTY'),2).alias('total_sales_qty'),
                           round(sum('SALES_AMT'),2).alias('total_sales_amt'),
                           round(sum('SALES_COST'),2).alias('total_sales_cost'),
                           round(sum('SALES_AMT') / sum('SALES_QTY'),2).alias('avg_sales_price'),
                           round(((sum('OUT_OF_STOCK_FLG') / 7) * 100),2).alias('percentage_Store_Stock'),
                           sum('OUT_OF_STOCK_FLG').alias('no_stock_instance'))



# Join df_calendar with df_inventory to get the inventory on end day of each week
df_inventory_end_week = df_inventory.join(df_calendar, df_inventory["CAL_DT"] == df_calendar["END_DAY_OF_WK"])

# Add 'end_week_inventory' and 'end_week_order_qty' columns
df_total = df_total.join(df_inventory_end_week.groupBy('END_DAY_OF_WK', 'PROD_KEY', 'STORE_KEY')
                                          .agg(first('INVENTORY_ON_HAND_QTY').alias('end_week_inventory'),
                                               first('INVENTORY_ON_ORDER_QTY').alias('end_week_order_qty')),
                         on=['END_DAY_OF_WK', 'PROD_KEY', 'STORE_KEY'])

# Add the 'total_low_stock_impact' column to df_merged
df_merged = df_merged.withColumn('total_low_stock_impact', col('Low_Stock_flg') + col('OUT_OF_STOCK_FLG'))

# Now, update the 'df_total' dataframe with this new column
df_total = df_total.join(df_merged.groupBy('END_DAY_OF_WK', 'PROD_KEY', 'STORE_KEY')
                         .agg(sum('total_low_stock_impact').alias('total_low_stock_impact')),
                         on=['END_DAY_OF_WK', 'PROD_KEY', 'STORE_KEY'])

# Add 'potential_low_stock_impact' and 'no_stock_impact' columns to df_merged
df_merged = df_merged.withColumn('potential_low_stock_impact', when(col('Low_Stock_flg') == 1, col('SALES_AMT')).otherwise(0))\
                     .withColumn('no_stock_impact', when(col('OUT_OF_STOCK_FLG') == 1, col('SALES_AMT')).otherwise(0))

# Now, update the 'df_total' dataframe with these new columns
df_total = df_total.join(df_merged.groupBy('END_DAY_OF_WK', 'PROD_KEY', 'STORE_KEY')
                         .agg(sum('potential_low_stock_impact').alias('potential_low_stock_impact'),
                              sum('no_stock_impact').alias('no_stock_impact'),
                              sum('Low_Stock_flg').alias('low_stock_instances'),
                              ),
                         on=['END_DAY_OF_WK', 'PROD_KEY', 'STORE_KEY'])

# Add 'weeks_of_supply' column and make it an integer number
df_total = df_total.withColumn('weeks_of_supply', (col('end_week_inventory') / col('total_sales_qty')).cast('integer'))


# Save the final dataframe as a parquet file to a new S3 output folder
df_total.repartition(1).write.mode("overwrite").option("compression","gzip").parquet(f"s3://midterm-result-ag/date={date_str}")

# Copy the files of store, product and calendar to the new output folder
import os

os.system(f"aws s3 cp s3://midterm-data-dump-ag/calendar_{date_str}.csv s3://midterm-result-ag/csv{date_str}/calendar_{date_str}.csv")
os.system(f"aws s3 cp s3://midterm-data-dump-ag/store_{date_str}.csv s3://midterm-result-ag/csv{date_str}/store_{date_str}.csv")
os.system(f"aws s3 cp s3://midterm-data-dump-ag/product_{date_str}.csv s3://midterm-result-ag/csv{date_str}/product_{date_str}.csv")