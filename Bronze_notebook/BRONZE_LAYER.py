# Databricks notebook source
# MAGIC %md
# MAGIC **IMPORTING RAW DATA FROM GITHUB**

# COMMAND ----------

github_url = " https://raw.githubusercontent.com/gops1703/Vehicle_management/refs/heads/main/source_data/Vehicle_details.csv"

# COMMAND ----------

import pandas as pd

#Creating a Dataframe
pdf = pd.read_csv(github_url)
df_vehicle_details = spark.createDataFrame(pdf)
display(df_vehicle_details.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **BRONZE LAYER**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, monotonically_increasing_id

# COMMAND ----------

#Creating Bronze dataframe with required columns
bronze_df = (df_vehicle_details
    .withColumn("_ingestion_date", current_timestamp())
    .withColumnRenamed("Fuel Type", "Fuel_Type")
    .withColumnRenamed("Seating Capacity","Seating_Capacity")
    .withColumnRenamed("Seller Type","Seller_Type")
    .withColumnRenamed("Max Power","Max_Power")
    .withColumnRenamed("Max Torque","Max_Torque")
    .withColumnRenamed("Fuel Tank Capacity","Fuel_Tank_Capacity")
    .withColumn("_row_id", monotonically_increasing_id())
)


# COMMAND ----------

display(bronze_df)

# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze.vehicle_details")

# COMMAND ----------

display(spark.table("bronze.vehicle_details"))