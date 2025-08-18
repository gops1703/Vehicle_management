# Databricks notebook source
from pyspark.sql.functions import monotonically_increasing_id,col

# COMMAND ----------

#Load silver_df from an existing Delta table

silver_df = spark.read.table("silver.vehicle_details")

# COMMAND ----------

# MAGIC %md
# MAGIC **VEHICLE DIMENSION**

# COMMAND ----------


dim_vehicle = (silver_df
    .select("Make","Model","Year","Fuel_Type","Transmission","Engine",
            "Max_Power","Max_Torque","Drivetrain","Seating_Capacity","Fuel_Tank_Capacity")
    .dropDuplicates()
    .withColumn("Vehicle_ID", monotonically_increasing_id())
)

dim_vehicle.write.format("delta").mode("overwrite").saveAsTable("gold.dim_vehicle")


# COMMAND ----------

display(dim_vehicle)

# COMMAND ----------

# MAGIC %md
# MAGIC **LOCATION DIMENSION**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# Step 1: Deduplicate by (Location, Color) and sort
location_df = (silver_df
    .select("Location", "Color")
    .dropDuplicates()
    .orderBy("Location", "Color")
)

# Step 2: Assign Location_ID (same for same Location, different across Locations)
windowSpec = Window.orderBy("Location")
dim_location = (location_df
    .withColumn("Location_ID", dense_rank().over(windowSpec))
)

dim_location.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable("gold.dim_location")





# COMMAND ----------

display(dim_location)

# COMMAND ----------

# MAGIC %md
# MAGIC **SELLER DIMENSION**

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

# Step 1: Deduplicate
seller_df = (silver_df
    .select("Seller_Type","Owner")
    .dropDuplicates()
    .orderBy("Seller_Type","Owner")
)

# Step 2: Assign one Seller_ID per Seller_Type
windowSpec = Window.orderBy("Seller_Type")
dim_seller = (seller_df
    .withColumn("Seller_ID", dense_rank().over(windowSpec))
)

dim_seller.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable("gold.dim_seller")


# COMMAND ----------

display(dim_seller)

# COMMAND ----------

# MAGIC %md
# MAGIC **FACT TABLE**

# COMMAND ----------

fact_sales = (silver_df
    .join(dim_vehicle, ["Make","Model","Year"], "inner")
    .join(dim_location, ["Location","Color"], "inner")
    .join(dim_seller, ["Seller_Type","Owner"], "inner")
    .selectExpr("monotonically_increasing_id() as Sale_ID",
                "Vehicle_ID","Seller_ID","Location_ID",
                "Price","Kilometer","current_date() as Date_Added")
)

fact_sales.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true").saveAsTable("gold.fact_vehicle_sales")

# COMMAND ----------

display(fact_sales)