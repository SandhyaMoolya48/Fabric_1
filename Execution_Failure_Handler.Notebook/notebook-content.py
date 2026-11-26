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

# PARAMETERS CELL ********************

exec_seq = 0
run_id = ""
error_message = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================================
# NB_12_EXECUTION_FAILURE_HANDLER
# ============================================================

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone

print("Execution Failure Handler initialized")

# ------------------------------------------------------------
# 1. VALIDATE PARAMETERS
# ------------------------------------------------------------

if not exec_seq or not run_id:
    raise ValueError("exec_seq and run_id are required")

exec_seq = int(exec_seq)
run_id = str(run_id)

if not error_message:
    error_message = "Pipeline execution failed. Check pipeline activity details."

# ------------------------------------------------------------
# 2. VERIFY EXECUTION RECORD
# ------------------------------------------------------------

log_df = spark.table("control_execution_log")

matching_count = (
    log_df
    .filter(
        (F.col("exec_seq") == exec_seq) &
        (F.col("run_id") == run_id)
    )
    .count()
)

if matching_count != 1:
    raise ValueError(
        f"Expected exactly one execution record, found {matching_count}"
    )

# ------------------------------------------------------------
# 3. UPDATE EXECUTION STATUS TO FAILED
# ------------------------------------------------------------

delta_log = DeltaTable.forName(
    spark,
    "control_execution_log"
)

delta_log.update(
    condition=(
        (F.col("exec_seq") == exec_seq) &
        (F.col("run_id") == run_id) &
        (F.upper(F.trim(F.col("execution_status"))) == "RUNNING")
    ),
    set={
        "execution_status": F.lit("FAILED"),
        "end_timestamp": F.current_timestamp(),
        "error_message": F.lit(str(error_message))
    }
)

# ------------------------------------------------------------
# 4. VERIFY FAILED STATUS
# ------------------------------------------------------------

updated_record = (
    spark.table("control_execution_log")
    .filter(
        (F.col("exec_seq") == exec_seq) &
        (F.col("run_id") == run_id)
    )
)

display(updated_record)

status = updated_record.first()["execution_status"]

if status != "FAILED":
    raise ValueError(
        f"Execution status update failed. Current status: {status}"
    )

print("====================================")
print("EXECUTION MARKED AS FAILED")
print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)
print("====================================")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
