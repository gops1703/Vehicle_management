## Vehicle Management – Data Engineering Project

This project demonstrates the **end-to-end Data Engineering pipeline** using **Databricks** with the **Medallion Architecture (Bronze → Silver → Gold)**.
The pipeline ingests raw vehicle sales data, performs cleaning and standardization, and finally creates a **star schema model** (dimension and fact tables) in Delta Lake for analytics and reporting.

---

### **1. Data Ingestion (Raw → Bronze Layer)**

* Raw CSV data (`Vehicle_details.csv`) is sourced directly from **GitHub** using its raw file URL.
* Data is first read into a **Pandas DataFrame** and then converted into a Spark DataFrame (`df_vehicle_details`).
* A **Bronze DataFrame** (`bronze_df`) is created with the following steps:

  * Add ingestion metadata (`_ingestion_date`, `_row_id`).
  * Standardize column names (e.g., `Fuel Type → Fuel_Type`).
* Data is saved as a **Delta table** in the **Bronze layer** (`bronze.vehicle_details`).

---

### **2. Data Cleansing & Standardization (Silver Layer)**

The Bronze data is refined to create a **Silver layer** table (`silver.vehicle_details`):

* **Deduplication** → Removed duplicates based on (`Make`, `Model`, `Year`, `Owner`).
* **Null filtering** → Dropped records with missing values in important business columns (e.g., Price, Year, Kilometer, Location, Seller\_Type).
* **Data type standardization**:

  * Converted `Kilometer` → integer (removed non-numeric chars).
  * Converted `Price` → double (removed currency symbols).
  * Converted categorical fields (`Fuel_Type`, `Transmission`) → lowercase trimmed strings.
* The cleaned data is written to the **Silver Delta Table** (`silver.vehicle_details`).

---

### **3. Data Modeling (Gold Layer – Star Schema)**

#### 🔹 Vehicle Dimension (`gold.dim_vehicle`)

* Attributes: Make, Model, Year, Fuel\_Type, Transmission, Engine, Max\_Power, Max\_Torque, Drivetrain, Seating\_Capacity, Fuel\_Tank\_Capacity.
* Ensures uniqueness with `.dropDuplicates()` and assigns a **Vehicle\_ID** using `monotonically_increasing_id()`.

#### 🔹 Location Dimension (`gold.dim_location`)

* Attributes: Location, Color.
* Deduplicated and sorted by Location and Color.
* Each **Location** gets a unique **Location\_ID** (shared across all colors for that location) using a **dense rank window function**.

#### 🔹 Seller Dimension (`gold.dim_seller`)

* Attributes: Seller\_Type, Owner.
* Deduplicated and sorted.
* Each **Seller\_Type** is assigned a **Seller\_ID** (same Seller\_ID for all owners under the same Seller\_Type) using **dense rank window function**.

#### 🔹 Fact Table (`gold.fact_vehicle_sales`)

* Captures transactional details of vehicle sales.
* Joins the **Silver table** with all dimension tables:

  * `Vehicle_ID` (from `dim_vehicle`)
  * `Location_ID` (from `dim_location`)
  * `Seller_ID` (from `dim_seller`)
* Measures: Price, Kilometer, Date\_Added.
* Each record assigned a unique **Sale\_ID**.

---

### **4. Benefits of the Model**

*  Ensures **clean, deduplicated, and standardized data**.
*  Implements a **Medallion architecture** for scalability.
*  Uses a **star schema design** (Fact + Dimensions) optimized for **BI and analytics** (e.g., Power BI, Tableau).
*  Built on **Delta Lake** for ACID transactions, schema enforcement, and time travel.

---

### **5. Final Gold Layer Tables**

* `gold.dim_vehicle`
* `gold.dim_location`
* `gold.dim_seller`
* `gold.fact_vehicle_sales`

These Gold tables can be directly consumed by **Power BI** or **other visualization tools** for reporting and analysis.




