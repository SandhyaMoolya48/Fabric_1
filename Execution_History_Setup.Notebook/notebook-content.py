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

exec_seq = 0
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType,
    DoubleType
)
from delta.tables import DeltaTable
from datetime import datetime, timezone
import uuid

# Execution log schema
execution_log_schema = StructType([
    StructField("exec_seq", LongType(), False),
    StructField("run_id", StringType(), False),
    StructField("start_timestamp", TimestampType(), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("execution_status", StringType(), True),
    StructField("error_message", StringType(), True)
])

# Execution summary schema
execution_summary_schema = StructType([
    StructField("exec_seq", LongType(), False),
    StructField("run_id", StringType(), False),
    StructField("audit_stage", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("metric_value", StringType(), True),
    StructField("audit_timestamp", TimestampType(), True)
])

print("Libraries and schemas loaded successfully")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# should not excute again only for fresh run 

# CELL ********************


# # Create empty execution log table
# empty_execution_log = spark.createDataFrame(
#     [],
#     execution_log_schema
# )

# (
#     empty_execution_log.write
#     .format("delta")
#     .mode("errorifexists")
#     .saveAsTable("control_execution_log")
# )

# print("control_execution_log created successfully")


# # Create empty execution summary table
# empty_execution_summary = spark.createDataFrame(
#     [],
#     execution_summary_schema
# )

# (
#     empty_execution_summary.write
#     .format("delta")
#     .mode("errorifexists")
#     .saveAsTable("control_execution_summary")
# )

# print("control_execution_summary created successfully")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Get the latest execution sequence
latest_seq = (
    spark.table("control_execution_log")
    .agg(F.max("exec_seq").alias("max_seq"))
    .first()["max_seq"]
)

# Generate the next execution sequence
exec_seq = 1 if latest_seq is None else latest_seq + 1

# Generate a unique run ID
run_id = str(uuid.uuid4())

# Capture execution start timestamp
start_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)
print("Start Timestamp:", start_timestamp)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


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

print("Execution start record saved successfully")

display(
    spark.table("control_execution_log")
    .orderBy("exec_seq")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def update_execution_status(status, error_message=None):

    if status not in ["SUCCESS", "FAILED"]:
        raise ValueError("Status must be SUCCESS or FAILED")

    end_timestamp = datetime.now(timezone.utc).replace(tzinfo=None)

    execution_table = DeltaTable.forName(
        spark,
        "control_execution_log"
    )

    execution_table.update(
        condition=(
            (F.col("exec_seq") == exec_seq) &
            (F.col("run_id") == run_id) &
            (F.col("execution_status") == "RUNNING")
        ),
        set={
            "end_timestamp": F.lit(end_timestamp).cast("timestamp"),
            "execution_status": F.lit(status),
            "error_message": F.lit(error_message)
        }
    )

    print(f"Execution {exec_seq} updated to {status}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Check the existing Gold audit summary schema

gold_audit_df = spark.table("gold_control_audit_summary")

print("Gold Audit Summary Schema:")
gold_audit_df.printSchema()

print("Sample Audit Records:")
display(gold_audit_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    TimestampType
)

execution_summary_schema = StructType([
    StructField("exec_seq", LongType(), False),
    StructField("run_id", StringType(), False),
    StructField("audit_stage", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("metric_value", StringType(), True),
    StructField("audit_timestamp", TimestampType(), True)
])

empty_execution_summary = spark.createDataFrame(
    [],
    execution_summary_schema
)

(
    empty_execution_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("control_execution_summary")
)

print("Execution summary schema updated successfully")

spark.table("control_execution_summary").printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

# Get the latest execution record
latest_execution = (
    spark.table("control_execution_log")
    .orderBy(F.col("exec_seq").desc())
    .first()
)

current_exec_seq = latest_execution["exec_seq"]
current_run_id = latest_execution["run_id"]

# Check whether this execution's summary already exists
existing_count = (
    spark.table("control_execution_summary")
    .filter(F.col("run_id") == current_run_id)
    .count()
)

if existing_count > 0:

    print(
        f"Summary already exists for execution "
        f"{current_exec_seq}. Skipping insert."
    )

else:

    # Read Gold audit summary
    gold_audit_df = spark.table("gold_control_audit_summary")

    # Add execution details
    execution_summary_df = (
        gold_audit_df
        .withColumn("exec_seq", F.lit(current_exec_seq).cast("long"))
        .withColumn("run_id", F.lit(current_run_id))
        .withColumn("metric_value", F.col("metric_value").cast("string"))
        .select(
            "exec_seq",
            "run_id",
            "audit_stage",
            "source_table",
            "metric_name",
            "metric_value",
            "audit_timestamp"
        )
    )

    # Append historical audit metrics
    (
        execution_summary_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("control_execution_summary")
    )

    print(
        f"Execution summary saved for execution "
        f"{current_exec_seq}"
    )

# Display saved execution history
display(
    spark.table("control_execution_summary")
    .filter(F.col("exec_seq") == current_exec_seq)
    .orderBy("audit_stage", "source_table", "metric_name")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


print("Execution Summary Count:")

display(
    spark.table("control_execution_summary")
    .groupBy("exec_seq", "run_id", "audit_stage")
    .count()
    .orderBy("exec_seq", "audit_stage")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from datetime import datetime, timezone

log_table = DeltaTable.forName(spark, "control_execution_log")

log_table.update(
    condition=(
        F.col("exec_seq").between(1, 7)
        & F.upper(F.trim(F.col("execution_status"))).isin(
            "RUNNING", "SUCCESS"
        )
    ),
    set={
        "execution_status": F.lit("FAILED"),
        "end_timestamp": F.current_timestamp(),
        "error_message": F.lit(
            "Historical test execution - not a completed end-to-end pipeline run"
        )
    }
)

print("Historical test execution statuses updated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("control_execution_log")
    .groupBy("exec_seq", "execution_status")
    .count()
    .orderBy("exec_seq", "execution_status")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from delta.tables import DeltaTable

table_name = "control_execution_log"

log_df = spark.table(table_name)

records_to_correct = log_df.filter(
    F.col("exec_seq") != 1
)

count_to_correct = records_to_correct.count()

print("Records to correct:", count_to_correct)

if count_to_correct != 6:
    raise ValueError(
        f"Expected 6 records, found {count_to_correct}. No changes made."
    )

if records_to_correct.filter(
    F.upper(F.trim(F.col("execution_status"))) != "FAILED"
).count() > 0:
    raise ValueError("Unexpected execution status. No changes made.")

delta_log = DeltaTable.forName(spark, table_name)

delta_log.update(
    condition=F.col("exec_seq") != 1,
    set={"exec_seq": F.lit(1)}
)

print("Successfully corrected all historical execution sequences.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


display(
    spark.table("control_execution_log")
    .select("exec_seq", "run_id", "execution_status")
    .orderBy("start_timestamp")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
