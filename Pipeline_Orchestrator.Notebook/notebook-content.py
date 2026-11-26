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

# exec_seq = 0
# run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# from pyspark.sql import functions as F
# from pyspark.sql import Row
# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     LongType,
#     StringType,
#     TimestampType
# )
# from delta.tables import DeltaTable

# from datetime import datetime, timezone
# import uuid

# print("Pipeline Orchestrator initialized successfully")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# # Get the latest completed or existing execution sequence
# latest_seq = (
#     spark.table("control_execution_log")
#     .agg(F.max("exec_seq").alias("max_seq"))
#     .first()["max_seq"]
# )

# # Generate the next execution sequence
# exec_seq = 1 if latest_seq is None else latest_seq + 1

# # Generate a unique execution ID
# run_id = str(uuid.uuid4())

# # Capture execution start timestamp
# start_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

# print("====================================")
# print("NEW PIPELINE EXECUTION")
# print("====================================")
# print("Execution Sequence:", exec_seq)
# print("Run ID:", run_id)
# print("Start Timestamp:", start_timestamp)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# # Define execution log schema
# execution_log_schema = StructType([
#     StructField("exec_seq", LongType(), False),
#     StructField("run_id", StringType(), False),
#     StructField("start_timestamp", TimestampType(), True),
#     StructField("end_timestamp", TimestampType(), True),
#     StructField("execution_status", StringType(), True),
#     StructField("error_message", StringType(), True)
# ])

# # Create RUNNING execution record
# execution_record = [
#     Row(
#         exec_seq=exec_seq,
#         run_id=run_id,
#         start_timestamp=start_timestamp,
#         end_timestamp=None,
#         execution_status="RUNNING",
#         error_message=None
#     )
# ]

# execution_df = spark.createDataFrame(
#     execution_record,
#     execution_log_schema
# )

# # Append execution record
# (
#     execution_df.write
#     .format("delta")
#     .mode("append")
#     .saveAsTable("control_execution_log")
# )

# print(f"Execution {exec_seq} started successfully")

# display(
#     spark.table("control_execution_log")
#     .orderBy("exec_seq")
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# # ======================================================
# # 4. DEFINE CHILD NOTEBOOKS
# # ======================================================

# notebooks_to_run = [
#     "NB_03_Bronze_Raw_Ingestion",
#     "NB_05_Silver_Data_Quality_Framework",
#     "NB_06_Gold_Reconciliation",
#     "NB_07_KRI_NonKRI_Classification",
#     "NB_08_Audit_And_Control_Summary"
# ]

# print("Notebooks scheduled for execution:")

# for notebook_name in notebooks_to_run:
#     print("-", notebook_name)


# # ======================================================
# # 5. DEFINE EXECUTION STATUS UPDATE FUNCTION
# # ======================================================

# def update_execution_status(status, error_message=None):

#     execution_delta = DeltaTable.forName(
#         spark,
#         "control_execution_log"
#     )

#     execution_delta.update(
#         condition=F.col("run_id") == run_id,
#         set={
#             "end_timestamp": F.current_timestamp(),
#             "execution_status": F.lit(status),
#             "error_message": (
#                 F.lit(error_message[:4000])
#                 if error_message
#                 else F.lit(None).cast("string")
#             )
#         }
#     )

#     print(f"Execution status updated to: {status}")


# # ======================================================
# # 6. EXECUTE CHILD NOTEBOOKS SEQUENTIALLY
# # ======================================================

# try:

#     for notebook_name in notebooks_to_run:

#         print("")
#         print("====================================")
#         print("STARTING:", notebook_name)
#         print("====================================")

#         child_result = notebookutils.notebook.run(
#             notebook_name,
#             3600,
#             {
#                 "exec_seq": exec_seq,
#                 "run_id": run_id
#             }
#         )

#         print("COMPLETED:", notebook_name)
#         print("Child notebook result:", child_result)


#     # ==================================================
#     # 7. READ CENTRAL GOLD AUDIT SUMMARY
#     # ==================================================

#     print("Reading central audit summary...")

#     gold_audit = spark.table(
#         "gold_control_audit_summary"
#     )

#     audit_required_columns = [
#         "exec_seq",
#         "run_id",
#         "audit_stage",
#         "source_table",
#         "metric_name",
#         "metric_value",
#         "audit_timestamp"
#     ]

#     missing_columns = [
#         c for c in audit_required_columns
#         if c not in gold_audit.columns
#     ]

#     if missing_columns:
#         raise ValueError(
#             f"Gold audit table is missing columns: {missing_columns}"
#         )

#     # Verify that the audit belongs to this execution
#     audit_mismatch = gold_audit.filter(
#         (F.col("exec_seq") != exec_seq)
#         | (F.col("run_id") != run_id)
#     ).limit(1).count()

#     if audit_mismatch > 0:
#         raise ValueError(
#             "Gold audit execution metadata mismatch."
#         )

#     audit_count = gold_audit.count()

#     if audit_count == 0:
#         raise ValueError("Gold audit summary contains no metrics.")

#     print("Audit metrics available:", audit_count)


#     # ==================================================
#     # 8. APPEND HISTORICAL EXECUTION SUMMARY
#     # ==================================================

#     history_table = spark.table(
#         "control_execution_summary"
#     )

#     # Prevent duplicate history insertion for the same run
#     existing_run_count = history_table.filter(
#         F.col("run_id") == run_id
#     ).limit(1).count()

#     if existing_run_count == 0:

#         history_to_append = gold_audit.select(
#             F.col("exec_seq").cast("long").alias("exec_seq"),
#             F.col("run_id").cast("string").alias("run_id"),
#             F.col("audit_stage"),
#             F.col("source_table"),
#             F.col("metric_name"),
#             F.col("metric_value").cast("string").alias("metric_value"),
#             F.col("audit_timestamp")
#         )

#         (
#             history_to_append.write
#             .format("delta")
#             .mode("append")
#             .saveAsTable("control_execution_summary")
#         )

#         print("Historical execution metrics appended.")

#     else:
#         print(
#             "Historical metrics already exist for this run. "
#             "Skipping duplicate insertion."
#         )


#     # ==================================================
#     # 9. VERIFY HISTORICAL SUMMARY
#     # ==================================================

#     saved_history_count = spark.table(
#         "control_execution_summary"
#     ).filter(
#         F.col("run_id") == run_id
#     ).count()

#     if saved_history_count != audit_count:
#         raise ValueError(
#             f"Historical summary count mismatch. "
#             f"Expected: {audit_count}, "
#             f"Found: {saved_history_count}"
#         )

#     print(
#         "Historical metrics verified:",
#         saved_history_count
#     )


#     # ==================================================
#     # 10. MARK EXECUTION SUCCESS
#     # ==================================================

#     update_execution_status("SUCCESS")

#     print("")
#     print("====================================")
#     print("PIPELINE EXECUTION SUCCESSFUL")
#     print("====================================")

#     print("Execution Sequence:", exec_seq)
#     print("Run ID:", run_id)
#     print("Audit Metrics:", saved_history_count)


# except Exception as e:

#     error_message = str(e)

#     print("")
#     print("====================================")
#     print("PIPELINE EXECUTION FAILED")
#     print("====================================")

#     print("Failed Execution:", exec_seq)
#     print("Error:", error_message)

#     update_execution_status(
#         "FAILED",
#         error_message
#     )

#     raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# above all the archestration using the note book 


# CELL ********************


# display(
#     spark.table("control_execution_log")
#     .orderBy("exec_seq")
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import json

# execution_parameters = {
#     "exec_seq": int(exec_seq),
#     "run_id": str(run_id)
# }

# notebookutils.notebook.exit(
#     json.dumps(execution_parameters)
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Parameter cell
exec_seq = 0
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# # ============================================================
# # NB_10_PIPELINE_ORCHESTRATOR
# # ============================================================



# from pyspark.sql import functions as F
# from pyspark.sql import Row
# from pyspark.sql.types import (
#     StructType,
#     StructField,
#     LongType,
#     StringType,
#     TimestampType
# )

# from datetime import datetime, timezone
# import uuid
# import json

# print("Pipeline Orchestrator initialized successfully")


# # ============================================================
# # 1. GET NEXT EXECUTION SEQUENCE
# # ============================================================

# latest_seq = (
#     spark.table("control_execution_log")
#     .agg(F.max("exec_seq").alias("max_seq"))
#     .first()["max_seq"]
# )

# exec_seq = 1 if latest_seq is None else int(latest_seq) + 1


# # ============================================================
# # 2. GENERATE RUN ID AND START TIMESTAMP
# # ============================================================

# run_id = str(uuid.uuid4())

# start_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

# print("Execution Sequence:", exec_seq)
# print("Run ID:", run_id)
# print("Start Timestamp:", start_timestamp)


# # ============================================================
# # 3. DEFINE EXECUTION LOG SCHEMA
# # ============================================================

# execution_log_schema = StructType([
#     StructField("exec_seq", LongType(), False),
#     StructField("run_id", StringType(), False),
#     StructField("start_timestamp", TimestampType(), True),
#     StructField("end_timestamp", TimestampType(), True),
#     StructField("execution_status", StringType(), True),
#     StructField("error_message", StringType(), True)
# ])


# # ============================================================
# # 4. INSERT RUNNING RECORD
# # ============================================================

# execution_record = [
#     Row(
#         exec_seq=exec_seq,
#         run_id=run_id,
#         start_timestamp=start_timestamp,
#         end_timestamp=None,
#         execution_status="RUNNING",
#         error_message=None
#     )
# ]

# execution_df = spark.createDataFrame(
#     execution_record,
#     execution_log_schema
# )

# (
#     execution_df.write
#     .format("delta")
#     .mode("append")
#     .saveAsTable("control_execution_log")
# )

# print("RUNNING record inserted successfully")


# # ============================================================
# # 5. VERIFY EXECUTION RECORD
# # ============================================================

# display(
#     spark.table("control_execution_log")
#     .filter(
#         (F.col("exec_seq") == exec_seq) &
#         (F.col("run_id") == run_id)
#     )
# )


# # ============================================================
# # 6. RETURN PARAMETERS TO PARENT PIPELINE
# # ============================================================

# execution_parameters = {
#     "exec_seq": int(exec_seq),
#     "run_id": str(run_id)
# }

# print("Returning execution parameters to parent pipeline")

# notebookutils.notebook.exit(
#     json.dumps(execution_parameters)
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================================
# NB_10_PIPELINE_ORCHESTRATOR
# ============================================================



from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType
)

from datetime import datetime, timezone
import uuid
import json

print("Pipeline Orchestrator initialized successfully")


# ============================================================
# 1. GET NEXT SUCCESSFUL EXECUTION SEQUENCE
# ============================================================

execution_log = spark.table("control_execution_log")

latest_successful_seq = (
    execution_log
    .filter(
        F.upper(F.trim(F.col("execution_status"))) == "SUCCESS"
    )
    .agg(F.max("exec_seq").alias("max_success_seq"))
    .first()["max_success_seq"]
)

exec_seq = (
    1 if latest_successful_seq is None
    else int(latest_successful_seq) + 1
)

print("Latest Successful Sequence:", latest_successful_seq)
print("Next Execution Sequence:", exec_seq)


# ============================================================
# 2. GENERATE UNIQUE RUN ID AND START TIMESTAMP
# ============================================================

run_id = str(uuid.uuid4())

start_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)
print("Start Timestamp:", start_timestamp)


# ============================================================
# 3. DEFINE EXECUTION LOG SCHEMA
# ============================================================

execution_log_schema = StructType([
    StructField("exec_seq", LongType(), False),
    StructField("run_id", StringType(), False),
    StructField("start_timestamp", TimestampType(), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("execution_status", StringType(), True),
    StructField("error_message", StringType(), True)
])


# ============================================================
# 4. INSERT RUNNING RECORD
# ============================================================

execution_record = [
    Row(
        exec_seq=exec_seq,
        run_id=run_id,
        start_timestamp=start_timestamp,
        end_timestamp=None,
        execution_status="RUNNING",
        error_message=None
    )
]

execution_df = spark.createDataFrame(
    execution_record,
    execution_log_schema
)

(
    execution_df.write
    .format("delta")
    .mode("append")
    .saveAsTable("control_execution_log")
)

print("RUNNING record inserted successfully")


# ============================================================
# 5. VERIFY EXECUTION RECORD
# ============================================================

display(
    spark.table("control_execution_log")
    .filter(
        (F.col("exec_seq") == exec_seq) &
        (F.col("run_id") == run_id)
    )
)


# ============================================================
# 6. RETURN PARAMETERS TO PARENT PIPELINE
# ============================================================

execution_parameters = {
    "exec_seq": int(exec_seq),
    "run_id": str(run_id)
}

print("Returning execution parameters to parent pipeline")

notebookutils.notebook.exit(
    json.dumps(execution_parameters)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# # ============================================================
# # NB_10 - EXECUTION SEQUENCE DIAGNOSTIC
# # ============================================================

# from pyspark.sql import functions as F

# execution_log = spark.table("control_execution_log")

# # Find the latest successfully completed execution
# latest_successful_seq = (
#     execution_log
#     .filter(
#         F.upper(F.trim(F.col("execution_status"))) == "SUCCESS"
#     )
#     .agg(F.max("exec_seq").alias("max_success_seq"))
#     .first()["max_success_seq"]
# )

# # Calculate the next execution sequence
# next_exec_seq = (
#     1 if latest_successful_seq is None
#     else int(latest_successful_seq) + 1
# )

# print("====================================")
# print("EXECUTION SEQUENCE VERIFICATION")
# print("====================================")
# print("Latest Successful Sequence:", latest_successful_seq)
# print("Next Execution Sequence:", next_exec_seq)
# print("====================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
