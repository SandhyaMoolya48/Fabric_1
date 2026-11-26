# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ceca31d0-c4b0-40a5-a1ed-4ef21beef54d",
# META       "default_lakehouse_name": "Final_lakehouse",
# META       "default_lakehouse_workspace_id": "8cfb619c-2004-4b43-98b3-42cf839fb476",
# META       "known_lakehouses": [
# META         {
# META           "id": "ceca31d0-c4b0-40a5-a1ed-4ef21beef54d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# Step 1: Define the source folder
source_path = "abfss://8cfb619c-2004-4b43-98b3-42cf839fb476@onelake.dfs.fabric.microsoft.com/ceca31d0-c4b0-40a5-a1ed-4ef21beef54d/Files/Raw_Data"

# Step 2: List all expected source files
source_files = [
    "customers.csv",
    "usage.csv",
    "billing.csv",
    "payments.csv",
    "subscriptions.csv",
    "network_events.csv"
]

# Step 3: Read and verify each CSV file
from pyspark.sql import functions as F

for file_name in source_files:

    file_path = f"{source_path}/{file_name}"

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .load(file_path)
    )

    print(f"\nFile Name: {file_name}")
    print(f"Total Rows: {df.count()}")
    print(f"Total Columns: {len(df.columns)}")
    print(f"Column Names: {df.columns}")

    df.show(5, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Check existing tables in the attached Lakehouse

existing_tables = spark.catalog.listTables()

print("Existing Lakehouse Tables:")

for table in existing_tables:
    print(table.name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Final_lakehouse.dbo.control_execution_summary LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
