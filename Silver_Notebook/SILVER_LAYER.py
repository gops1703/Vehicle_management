# Databricks notebook source
from pyspark.sql.functions import col, regexp_replace, trim, lower

# COMMAND ----------



silver_df = (spark.table("bronze.vehicle_details")
    # Deduplication
    .dropDuplicates(["Make", "Model", "Year", "Owner"])
    
    # Null checks
    .filter(col("Make").isNotNull() & col("Model").isNotNull() & col("Price").isNotNull() & col("Year").isNotNull() & col("Kilometer").isNotNull() & col("Fuel_Type").isNotNull() & col("Transmission").isNotNull() & col("Location").isNotNull() & col("Color").isNotNull() & col("Owner").isNotNull() & col("Seller_Type").isNotNull() & col("Engine").isNotNull() & col("Max_Power").isNotNull() & col("Max_Torque").isNotNull() & col("Drivetrain").isNotNull() & col("Length").isNotNull() & col("Width").isNotNull() & col("Height").isNotNull() & col("Seating_Capacity").isNotNull() & col("Fuel_Tank_Capacity").isNotNull())
    
    # Standardize Kilometer -> numeric
    .withColumn("Kilometer", regexp_replace(col("Kilometer"), "[^0-9]", "").cast("int"))
    
    # Standardize Price -> double
    .withColumn("Price", regexp_replace(col("Price"), "[^0-9]", "").cast("double"))
    
    # Standardize Fuel Type -> lower case
    .withColumn("Fuel_Type", trim(lower(col("Fuel_Type"))))
    
    # Standardize Transmission -> lower case
    .withColumn("Transmission", trim(lower(col("Transmission"))))
)





# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable("silver.vehicle_details")