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


# from pyspark.sql import functions as F
# from datetime import datetime

# # Source folder
# source_path = "abfss://8cfb619c-2004-4b43-98b3-42cf839fb476@onelake.dfs.fabric.microsoft.com/ceca31d0-c4b0-40a5-a1ed-4ef21beef54d/Files/Raw_Data"

# # Source files and Bronze table names
# source_tables = {
#     "customers.csv": "bronze_customers",
#     "usage.csv": "bronze_usage",
#     "billing.csv": "bronze_billing",
#     "payments.csv": "bronze_payments",
#     "subscriptions.csv": "bronze_subscriptions",
#     "network_events.csv": "bronze_network_events"
# }

# # Ingest each source file into Bronze
# for file_name, table_name in source_tables.items():

#     file_path = f"{source_path}/{file_name}"

#     df = (
#         spark.read
#         .format("csv")
#         .option("header", "true")
#         .option("inferSchema", "false")
#         .option("mode", "PERMISSIVE")
#         .load(file_path)
#     )

#     # Add ingestion metadata
#     df = (
#         df
#         .withColumn("_source_file", F.lit(file_name))
#         .withColumn("_ingestion_timestamp", F.current_timestamp())
#         .withColumn("_ingestion_date", F.current_date())
#     )

#     # Save as a Delta table
#     (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .saveAsTable(table_name)
#     )

#     print(f"{table_name} created successfully")
#     print(f"Source records: {df.count()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Validation

# CELL ********************


# bronze_tables = [
#     "bronze_customers",
#     "bronze_usage",
#     "bronze_billing",
#     "bronze_payments",
#     "bronze_subscriptions",
#     "bronze_network_events"
# ]

# for table_name in bronze_tables:

#     df = spark.table(table_name)

#     print(
#         f"{table_name}: "
#         f"{df.count()} records, "
#         f"{len(df.columns)} columns"
#     )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# PARAMETERS CELL ********************

# Parameters passed by NB_10_Pipeline_Orchestrator

exec_seq = 0
run_id = ""


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# from pyspark.sql import functions as F
# from datetime import datetime, timezone
# import uuid

# # ------------------------------------------------------
# # 1. CONFIGURATION
# # ------------------------------------------------------

# source_path = (
#     "abfss://8cfb619c-2004-4b43-98b3-42cf839fb476"
#     "@onelake.dfs.fabric.microsoft.com/"
#     "ceca31d0-c4b0-40a5-a1ed-4ef21beef54d/"
#     "Files/Raw_Data"
# )

# source_tables = {
#     "customers.csv": "bronze_customers",
#     "usage.csv": "bronze_usage",
#     "billing.csv": "bronze_billing",
#     "payments.csv": "bronze_payments",
#     "subscriptions.csv": "bronze_subscriptions",
#     "network_events.csv": "bronze_network_events"
# }

# # Generate one unique ID for this ingestion execution
# run_id = str(uuid.uuid4())

# # Keep the same timestamp for all six sources
# run_timestamp = datetime.now(timezone.utc)

# print("Bronze ingestion started")
# print("Run ID:", run_id)
# print("Run timestamp:", run_timestamp)


# # ------------------------------------------------------
# # 2. READ AND VALIDATE ALL SOURCE FILES
# # No Bronze tables are overwritten in this section.
# # ------------------------------------------------------

# prepared_dataframes = {}
# source_counts = {}

# for file_name, table_name in source_tables.items():

#     file_path = f"{source_path}/{file_name}"

#     print(f"\nReading: {file_name}")

#     df = (
#         spark.read
#         .format("csv")
#         .option("header", "true")
#         .option("inferSchema", "false")
#         .option("mode", "PERMISSIVE")
#         .load(file_path)
#     )

#     # Check that the source has columns
#     if len(df.columns) == 0:
#         raise ValueError(
#             f"Source file has no columns: {file_name}"
#         )

#     # Count source records
#     record_count = df.count()

#     # Prevent accidental replacement with an empty dataset
#     if record_count == 0:
#         raise ValueError(
#             f"Source file is empty: {file_name}. "
#             "Bronze ingestion stopped."
#         )

#     # Add ingestion metadata
#     df = (
#         df
#         .withColumn("_source_file", F.lit(file_name))
#         .withColumn(
#             "_ingestion_timestamp",
#             F.lit(run_timestamp).cast("timestamp")
#         )
#         .withColumn(
#             "_ingestion_date",
#             F.to_date(F.lit(run_timestamp))
#         )
#         .withColumn("_run_id", F.lit(run_id))
#     )

#     prepared_dataframes[table_name] = df
#     source_counts[table_name] = record_count

#     print(f"Validated: {file_name}")
#     print(f"Records: {record_count}")


# # ------------------------------------------------------
# # 3. WRITE ALL VALIDATED DATA TO BRONZE
# # Full refresh using overwrite
# # ------------------------------------------------------

# for table_name, df in prepared_dataframes.items():

#     (
#         df.write
#         .format("delta")
#         .mode("overwrite")
#         .option("overwriteSchema", "true")
#         .saveAsTable(table_name)
#     )

#     print(f"{table_name} loaded successfully")


# # ------------------------------------------------------
# # 4. INGESTION SUMMARY
# # ------------------------------------------------------

# print("\n========== BRONZE INGESTION SUMMARY ==========")

# total_records = 0

# for table_name, record_count in source_counts.items():

#     print(f"{table_name}: {record_count}")

#     total_records += record_count

# print("----------------------------------------------")
# print("Total source records:", total_records)
# print("Run ID:", run_id)
# print("Bronze ingestion completed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from datetime import datetime, timezone

# ------------------------------------------------------
# 1. EXECUTION VALIDATION
# ------------------------------------------------------

if exec_seq is None or run_id is None:
    raise ValueError(
        "exec_seq and run_id must be provided by the orchestrator."
    )

print("Bronze ingestion started")
print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)

# Keep the same timestamp for all six sources
run_timestamp = datetime.now(timezone.utc)


# ------------------------------------------------------
# 2. CONFIGURATION
# ------------------------------------------------------

source_path = (
    "abfss://8cfb619c-2004-4b43-98b3-42cf839fb476"
    "@onelake.dfs.fabric.microsoft.com/"
    "ceca31d0-c4b0-40a5-a1ed-4ef21beef54d/"
    "Files/Raw_Data"
)

source_tables = {
    "customers.csv": "bronze_customers",
    "usage.csv": "bronze_usage",
    "billing.csv": "bronze_billing",
    "payments.csv": "bronze_payments",
    "subscriptions.csv": "bronze_subscriptions",
    "network_events.csv": "bronze_network_events"
}


# ------------------------------------------------------
# 3. READ AND VALIDATE ALL SOURCE FILES
# No Bronze tables are overwritten in this section.
# ------------------------------------------------------

prepared_dataframes = {}
source_counts = {}

for file_name, table_name in source_tables.items():

    file_path = f"{source_path}/{file_name}"

    print(f"\nReading: {file_name}")

    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .load(file_path)
    )

    if len(df.columns) == 0:
        raise ValueError(
            f"Source file has no columns: {file_name}"
        )

    record_count = df.count()

    if record_count == 0:
        raise ValueError(
            f"Source file is empty: {file_name}. "
            "Bronze ingestion stopped."
        )

    # Add ingestion metadata
    df = (
        df
        .withColumn("_source_file", F.lit(file_name))
        .withColumn(
            "_ingestion_timestamp",
            F.lit(run_timestamp).cast("timestamp")
        )
        .withColumn(
            "_ingestion_date",
            F.to_date(F.lit(run_timestamp))
        )
        .withColumn("_run_id", F.lit(run_id))
        .withColumn("_exec_seq", F.lit(exec_seq).cast("long"))
    )

    prepared_dataframes[table_name] = df
    source_counts[table_name] = record_count

    print(f"Validated: {file_name}")
    print(f"Records: {record_count}")


# ------------------------------------------------------
# 4. WRITE ALL VALIDATED DATA TO BRONZE
# Full refresh using overwrite
# ------------------------------------------------------

for table_name, df in prepared_dataframes.items():

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

    print(f"{table_name} loaded successfully")


# ------------------------------------------------------
# 5. INGESTION SUMMARY
# ------------------------------------------------------

print("\n========== BRONZE INGESTION SUMMARY ==========")

total_records = 0

for table_name, record_count in source_counts.items():

    print(f"{table_name}: {record_count}")

    total_records += record_count

print("----------------------------------------------")
print("Execution Sequence:", exec_seq)
print("Total source records:", total_records)
print("Run ID:", run_id)
print("Bronze ingestion completed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
