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

# # ------------------------------------------------------
# # 1. READ EXISTING TABLES
# # ------------------------------------------------------

# silver_audit = spark.table("silver_audit_summary")

# reconciliation = spark.table(
#     "gold_usage_billing_reconciliation"
# )

# kri_df = spark.table("gold_kri_records")

# non_kri_df = spark.table("gold_non_kri_records")


# # ------------------------------------------------------
# # 2. SILVER AUDIT METRICS
# # One set of metrics for each source table
# # ------------------------------------------------------

# silver_metrics = silver_audit.selectExpr(
#     "source_table",
#     """
#     stack(
#         8,
#         'source_count', cast(source_count as string),
#         'duplicate_count', cast(duplicate_count as string),
#         'invalid_pk_count', cast(invalid_pk_count as string),
#         'invalid_date_count', cast(invalid_date_count as string),
#         'invalid_numeric_count', cast(invalid_numeric_count as string),
#         'filter_count', cast(filter_count as string),
#         'balance_count', cast(balance_count as string),
#         'processing_status', cast(processing_status as string)
#     ) as (metric_name, metric_value)
# """).select(
#     F.lit("SILVER").alias("audit_stage"),
#     "source_table",
#     "metric_name",
#     "metric_value"
# )


# # ------------------------------------------------------
# # 3. RECONCILIATION STATUS METRICS
# # ------------------------------------------------------

# reconciliation_metrics = (
#     reconciliation
#     .groupBy("reconciliation_status")
#     .count()
#     .select(
#         F.lit("RECONCILIATION").alias("audit_stage"),
#         F.lit("ALL_SOURCES").alias("source_table"),
#         F.concat(
#             F.lit("reconciliation_status_"),
#             F.col("reconciliation_status")
#         ).alias("metric_name"),
#         F.col("count").cast("string").alias("metric_value")
#     )
# )


# # ------------------------------------------------------
# # 4. AMOUNT COMPARISON METRICS
# # ------------------------------------------------------

# amount_metrics = (
#     reconciliation
#     .groupBy("amount_comparison_status")
#     .count()
#     .select(
#         F.lit("RECONCILIATION").alias("audit_stage"),
#         F.lit("ALL_SOURCES").alias("source_table"),
#         F.concat(
#             F.lit("amount_comparison_"),
#             F.col("amount_comparison_status")
#         ).alias("metric_name"),
#         F.col("count").cast("string").alias("metric_value")
#     )
# )


# # ------------------------------------------------------
# # 5. TOTAL RECONCILIATION RECORDS
# # ------------------------------------------------------

# total_reconciliation = reconciliation.agg(
#     F.count("*").alias("total_count")
# ).select(
#     F.lit("RECONCILIATION").alias("audit_stage"),
#     F.lit("ALL_SOURCES").alias("source_table"),
#     F.lit("total_reconciliation_records").alias("metric_name"),
#     F.col("total_count").cast("string").alias("metric_value")
# )


# # ------------------------------------------------------
# # 6. KRI CLASSIFICATION METRICS
# # ------------------------------------------------------

# kri_classification_metrics = (
#     kri_df
#     .groupBy("classification")
#     .count()
#     .select(
#         F.lit("KRI").alias("audit_stage"),
#         F.lit("ALL_SOURCES").alias("source_table"),
#         F.concat(
#             F.lit("classification_"),
#             F.col("classification")
#         ).alias("metric_name"),
#         F.col("count").cast("string").alias("metric_value")
#     )
# )


# # ------------------------------------------------------
# # 7. NON-KRI TOTAL
# # ------------------------------------------------------

# non_kri_total = non_kri_df.agg(
#     F.count("*").alias("total_count")
# ).select(
#     F.lit("KRI").alias("audit_stage"),
#     F.lit("ALL_SOURCES").alias("source_table"),
#     F.lit("total_non_kri_records").alias("metric_name"),
#     F.col("total_count").cast("string").alias("metric_value")
# )


# # ------------------------------------------------------
# # 8. KRI ID-WISE COUNTS
# # ------------------------------------------------------

# kri_id_metrics = (
#     kri_df
#     .groupBy("kri_id")
#     .count()
#     .select(
#         F.lit("KRI").alias("audit_stage"),
#         F.lit("ALL_SOURCES").alias("source_table"),
#         F.concat(
#             F.lit("kri_id_"),
#             F.col("kri_id")
#         ).alias("metric_name"),
#         F.col("count").cast("string").alias("metric_value")
#     )
# )


# # ------------------------------------------------------
# # 9. COMBINE ALL AUDIT METRICS
# # ------------------------------------------------------

# audit_summary = (
#     silver_metrics
#     .unionByName(reconciliation_metrics)
#     .unionByName(amount_metrics)
#     .unionByName(total_reconciliation)
#     .unionByName(kri_classification_metrics)
#     .unionByName(non_kri_total)
#     .unionByName(kri_id_metrics)
#     .withColumn(
#         "audit_timestamp",
#         F.current_timestamp()
#     )
# )


# # ------------------------------------------------------
# # 10. SAVE CENTRAL AUDIT TABLE
# # ------------------------------------------------------

# (
#     audit_summary.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable("gold_control_audit_summary")
# )

# print("Central audit summary created successfully.")


# # ------------------------------------------------------
# # 11. DISPLAY AUDIT SUMMARY
# # ------------------------------------------------------

# display(
#     spark.table("gold_control_audit_summary")
#     .orderBy("audit_stage", "source_table", "metric_name")
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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


from pyspark.sql import functions as F

# ======================================================
# 1. EXECUTION VALIDATION
# ======================================================

if not exec_seq or not run_id:
    raise ValueError(
        "exec_seq and run_id must be provided by the orchestrator."
    )

exec_seq = int(exec_seq)
run_id = str(run_id)

print("CENTRAL AUDIT SUMMARY STARTED")
print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)


# ======================================================
# 2. READ EXISTING TABLES
# ======================================================

silver_audit = spark.table("silver_audit_summary")

reconciliation = spark.table(
    "gold_usage_billing_reconciliation"
)

kri_df = spark.table("gold_kri_records")

non_kri_df = spark.table("gold_non_kri_records")


# ======================================================
# 3. VERIFY EXECUTION METADATA
# ======================================================

def verify_execution(df, table_name, seq_column, id_column):

    required_columns = [seq_column, id_column]

    missing_columns = [
        c for c in required_columns
        if c not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing execution columns: "
            f"{missing_columns}"
        )

    mismatch_count = df.filter(
        (F.col(id_column) != run_id)
        | (F.col(seq_column) != exec_seq)
        | F.col(id_column).isNull()
        | F.col(seq_column).isNull()
    ).limit(1).count()

    if mismatch_count > 0:
        raise ValueError(
            f"Execution metadata mismatch in {table_name}"
        )

    print(f"{table_name}: Execution verified")


# Silver audit uses exec_seq and run_id
verify_execution(
    silver_audit,
    "silver_audit_summary",
    "exec_seq",
    "run_id"
)

# Gold tables use _exec_seq and _run_id
verify_execution(
    reconciliation,
    "gold_usage_billing_reconciliation",
    "_exec_seq",
    "_run_id"
)

verify_execution(
    kri_df,
    "gold_kri_records",
    "_exec_seq",
    "_run_id"
)

verify_execution(
    non_kri_df,
    "gold_non_kri_records",
    "_exec_seq",
    "_run_id"
)


# ======================================================
# 4. SILVER AUDIT METRICS
# One set of metrics for each source table
# ======================================================

silver_metrics = silver_audit.selectExpr(
    "source_table",
    """
    stack(
        8,
        'source_count', cast(source_count as string),
        'duplicate_count', cast(duplicate_count as string),
        'invalid_pk_count', cast(invalid_pk_count as string),
        'invalid_date_count', cast(invalid_date_count as string),
        'invalid_numeric_count', cast(invalid_numeric_count as string),
        'filter_count', cast(filter_count as string),
        'balance_count', cast(balance_count as string),
        'processing_status', cast(processing_status as string)
    ) as (metric_name, metric_value)
    """
).select(
    F.lit("SILVER").alias("audit_stage"),
    "source_table",
    "metric_name",
    "metric_value"
)


# ======================================================
# 5. RECONCILIATION STATUS METRICS
# ======================================================

reconciliation_metrics = (
    reconciliation
    .groupBy("reconciliation_status")
    .count()
    .select(
        F.lit("RECONCILIATION").alias("audit_stage"),
        F.lit("ALL_SOURCES").alias("source_table"),
        F.concat(
            F.lit("reconciliation_status_"),
            F.col("reconciliation_status")
        ).alias("metric_name"),
        F.col("count").cast("string").alias("metric_value")
    )
)


# ======================================================
# 6. AMOUNT COMPARISON METRICS
# ======================================================

amount_metrics = (
    reconciliation
    .groupBy("amount_comparison_status")
    .count()
    .select(
        F.lit("RECONCILIATION").alias("audit_stage"),
        F.lit("ALL_SOURCES").alias("source_table"),
        F.concat(
            F.lit("amount_comparison_"),
            F.col("amount_comparison_status")
        ).alias("metric_name"),
        F.col("count").cast("string").alias("metric_value")
    )
)


# ======================================================
# 7. TOTAL RECONCILIATION RECORDS
# ======================================================

total_reconciliation = reconciliation.agg(
    F.count("*").alias("total_count")
).select(
    F.lit("RECONCILIATION").alias("audit_stage"),
    F.lit("ALL_SOURCES").alias("source_table"),
    F.lit("total_reconciliation_records").alias("metric_name"),
    F.col("total_count").cast("string").alias("metric_value")
)


# ======================================================
# 8. KRI CLASSIFICATION METRICS
# ======================================================

kri_classification_metrics = (
    kri_df
    .groupBy("classification")
    .count()
    .select(
        F.lit("KRI").alias("audit_stage"),
        F.lit("ALL_SOURCES").alias("source_table"),
        F.concat(
            F.lit("classification_"),
            F.col("classification")
        ).alias("metric_name"),
        F.col("count").cast("string").alias("metric_value")
    )
)


# ======================================================
# 9. NON-KRI TOTAL
# ======================================================

non_kri_total = non_kri_df.agg(
    F.count("*").alias("total_count")
).select(
    F.lit("KRI").alias("audit_stage"),
    F.lit("ALL_SOURCES").alias("source_table"),
    F.lit("total_non_kri_records").alias("metric_name"),
    F.col("total_count").cast("string").alias("metric_value")
)


# ======================================================
# 10. KRI ID-WISE COUNTS
# ======================================================

kri_id_metrics = (
    kri_df
    .groupBy("kri_id")
    .count()
    .select(
        F.lit("KRI").alias("audit_stage"),
        F.lit("ALL_SOURCES").alias("source_table"),
        F.concat(
            F.lit("kri_id_"),
            F.col("kri_id")
        ).alias("metric_name"),
        F.col("count").cast("string").alias("metric_value")
    )
)


# ======================================================
# 11. COMBINE ALL AUDIT METRICS
# ======================================================

audit_summary = (
    silver_metrics
    .unionByName(reconciliation_metrics)
    .unionByName(amount_metrics)
    .unionByName(total_reconciliation)
    .unionByName(kri_classification_metrics)
    .unionByName(non_kri_total)
    .unionByName(kri_id_metrics)
    .withColumn("exec_seq", F.lit(exec_seq).cast("long"))
    .withColumn("run_id", F.lit(run_id))
    .withColumn(
        "audit_timestamp",
        F.current_timestamp()
    )
)


# ======================================================
# 12. VERIFY AUDIT METRICS
# ======================================================

audit_metric_count = audit_summary.count()

if audit_metric_count == 0:
    raise ValueError("No audit metrics were generated.")

print("Total audit metrics generated:", audit_metric_count)


# ======================================================
# 13. SAVE CENTRAL AUDIT TABLE
# ======================================================

(
    audit_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_control_audit_summary")
)

print("Central audit summary created successfully.")


# ======================================================
# 14. VERIFY SAVED AUDIT TABLE
# ======================================================

saved_audit = spark.table("gold_control_audit_summary")

saved_count = saved_audit.count()

if saved_count != audit_metric_count:
    raise ValueError(
        f"Audit save count mismatch. "
        f"Generated: {audit_metric_count}, "
        f"Saved: {saved_count}"
    )

print("Saved audit metrics:", saved_count)
print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)


# ======================================================
# 15. DISPLAY AUDIT SUMMARY
# ======================================================

display(
    saved_audit
    .orderBy("audit_stage", "source_table", "metric_name")
)

print("CENTRAL AUDIT SUMMARY COMPLETED SUCCESSFULLY")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_df = spark.table("gold_control_audit_summary")

display(
    audit_df
    .select("exec_seq", "run_id")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
