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

# # ------------------------------------------------------
# # 1. READ SILVER BALANCE TABLES
# # ------------------------------------------------------

# usage_df = spark.table("silver_usage_balance")
# billing_df = spark.table("silver_billing_balance")

# print("Usage Balance Count:", usage_df.count())
# print("Billing Balance Count:", billing_df.count())


# # ------------------------------------------------------
# # 2. AGGREGATE USAGE
# # Group by customer_id and billing_period
# # ------------------------------------------------------

# usage_agg = (
#     usage_df
#     .withColumn(
#         "usage_amount_numeric",
#         F.expr("try_cast(usage_amount AS DOUBLE)")
#     )
#     .groupBy("customer_id", "billing_period")
#     .agg(
#         F.sum("usage_amount_numeric").alias("total_usage_amount"),
#         F.count("*").alias("usage_record_count")
#     )
#     .withColumn("_usage_exists", F.lit(1))
# )



# # ------------------------------------------------------
# # 3. AGGREGATE BILLING
# # Compare usage amount with billed amount (before tax)
# # ------------------------------------------------------

# billing_agg = (
#     billing_df
#     .withColumn(
#         "billed_amount_decimal",
#         F.expr("try_cast(billed_amount AS DECIMAL(18,2))")
#     )
#     .groupBy("customer_id", "billing_period")
#     .agg(
#         F.sum("billed_amount_decimal").alias("total_billed_amount"),
#         F.count("*").alias("billing_record_count")
#     )
#     .withColumn("_billing_exists", F.lit(1))
# )



# # ------------------------------------------------------
# # 4. FULL OUTER JOIN
# # ------------------------------------------------------

# reconciliation_df = (
#     usage_agg.alias("u")
#     .join(
#         billing_agg.alias("b"),
#         on=["customer_id", "billing_period"],
#         how="full_outer"
#     )
# )


# # ------------------------------------------------------
# # 5. CLASSIFY YY, YN, NY
# # ------------------------------------------------------

# reconciliation_df = (
#     reconciliation_df
#     .withColumn(
#         "reconciliation_status",
#         F.when(
#             F.col("_usage_exists").isNotNull()
#             & F.col("_billing_exists").isNotNull(),
#             "YY"
#         )
#         .when(
#             F.col("_usage_exists").isNotNull(),
#             "YN"
#         )
#         .when(
#             F.col("_billing_exists").isNotNull(),
#             "NY"
#         )
#     )
# )


# # ------------------------------------------------------
# # 6. CALCULATE AMOUNT DIFFERENCE
# # Usage Amount - Billing Amount
# # ------------------------------------------------------

# reconciliation_df = (
#     reconciliation_df
#     .withColumn(
#         "amount_difference",
#         F.when(
#             F.col("reconciliation_status") == "YY",
#             F.round(
#                 F.col("total_usage_amount")
#                 - F.col("total_billed_amount"),
#                 2
#             )
#         )
#     )
#     .withColumn(
#         "amount_comparison_status",
#         F.when(
#             F.col("reconciliation_status") != "YY",
#             "NOT_COMPARABLE"
#         )
#         .when(
#             F.col("amount_difference") == 0,
#             "AMOUNT_MATCH"
#         )
#         .otherwise("AMOUNT_MISMATCH")
#     )
# )


# # ------------------------------------------------------
# # 7. SELECT FINAL GOLD COLUMNS
# # ------------------------------------------------------

# gold_reconciliation = reconciliation_df.select(
#     "customer_id",
#     "billing_period",
#     "total_usage_amount",
#     "total_billed_amount",
#     "usage_record_count",
#     "billing_record_count",
#     "reconciliation_status",
#     "amount_difference",
#     "amount_comparison_status"
# )


# # ------------------------------------------------------
# # 8. SAVE GOLD DELTA TABLE
# # ------------------------------------------------------

# (
#     gold_reconciliation.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable("gold_usage_billing_reconciliation")
# )

# print("Gold reconciliation table created successfully.")


# # ------------------------------------------------------
# # 9. DISPLAY RECONCILIATION RESULTS
# # ------------------------------------------------------

# display(
#     spark.table("gold_usage_billing_reconciliation")
#     .orderBy("customer_id", "billing_period")
# )


# # ------------------------------------------------------
# # 10. RECONCILIATION SUMMARY
# # ------------------------------------------------------

# reconciliation_summary = (
#     spark.table("gold_usage_billing_reconciliation")
#     .groupBy("reconciliation_status")
#     .agg(
#         F.count("*").alias("record_count"),
#         F.sum("total_usage_amount").alias("total_usage_amount"),
#         F.sum("total_billed_amount").alias("total_billed_amount")
#     )
#     .orderBy("reconciliation_status")
# )

# display(reconciliation_summary)


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

print("GOLD RECONCILIATION STARTED")
print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)


# ======================================================
# 2. READ SILVER BALANCE TABLES
# ======================================================

usage_df = spark.table("silver_usage_balance")
billing_df = spark.table("silver_billing_balance")

# Verify execution metadata
for table_name, df in [
    ("silver_usage_balance", usage_df),
    ("silver_billing_balance", billing_df)
]:

    required_columns = ["_run_id", "_exec_seq"]

    missing_columns = [
        c for c in required_columns
        if c not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing execution metadata: "
            f"{missing_columns}"
        )

    mismatch_count = df.filter(
        (F.col("_run_id") != run_id)
        | (F.col("_exec_seq") != exec_seq)
        | F.col("_run_id").isNull()
        | F.col("_exec_seq").isNull()
    ).limit(1).count()

    if mismatch_count > 0:
        raise ValueError(
            f"Execution metadata mismatch in {table_name}"
        )

    print(f"{table_name}: Execution verified")

print("Usage Balance Count:", usage_df.count())
print("Billing Balance Count:", billing_df.count())


# ======================================================
# 3. AGGREGATE USAGE
# Group by customer_id and billing_period
# ======================================================

usage_agg = (
    usage_df
    .withColumn(
        "usage_amount_numeric",
        F.expr("try_cast(usage_amount AS DOUBLE)")
    )
    .groupBy("customer_id", "billing_period")
    .agg(
        F.sum("usage_amount_numeric").alias("total_usage_amount"),
        F.count("*").alias("usage_record_count")
    )
    .withColumn("_usage_exists", F.lit(1))
)


# ======================================================
# 4. AGGREGATE BILLING
# Compare usage amount with billed amount (before tax)
# ======================================================

billing_agg = (
    billing_df
    .withColumn(
        "billed_amount_decimal",
        F.expr("try_cast(billed_amount AS DECIMAL(18,2))")
    )
    .groupBy("customer_id", "billing_period")
    .agg(
        F.sum("billed_amount_decimal").alias("total_billed_amount"),
        F.count("*").alias("billing_record_count")
    )
    .withColumn("_billing_exists", F.lit(1))
)


# ======================================================
# 5. FULL OUTER JOIN
# ======================================================

reconciliation_df = (
    usage_agg.alias("u")
    .join(
        billing_agg.alias("b"),
        on=["customer_id", "billing_period"],
        how="full_outer"
    )
)


# ======================================================
# 6. CLASSIFY YY, YN, NY
# ======================================================

reconciliation_df = (
    reconciliation_df
    .withColumn(
        "reconciliation_status",
        F.when(
            F.col("_usage_exists").isNotNull()
            & F.col("_billing_exists").isNotNull(),
            "YY"
        )
        .when(
            F.col("_usage_exists").isNotNull(),
            "YN"
        )
        .when(
            F.col("_billing_exists").isNotNull(),
            "NY"
        )
    )
)


# ======================================================
# 7. CALCULATE AMOUNT DIFFERENCE
# Usage Amount - Billing Amount
# ======================================================

reconciliation_df = (
    reconciliation_df
    .withColumn(
        "amount_difference",
        F.when(
            F.col("reconciliation_status") == "YY",
            F.round(
                F.col("total_usage_amount")
                - F.col("total_billed_amount"),
                2
            )
        )
    )
    .withColumn(
        "amount_comparison_status",
        F.when(
            F.col("reconciliation_status") != "YY",
            "NOT_COMPARABLE"
        )
        .when(
            F.col("amount_difference") == 0,
            "AMOUNT_MATCH"
        )
        .otherwise("AMOUNT_MISMATCH")
    )
)


# ======================================================
# 8. SELECT FINAL GOLD COLUMNS
# ======================================================

gold_reconciliation = reconciliation_df.select(
    "customer_id",
    "billing_period",
    "total_usage_amount",
    "total_billed_amount",
    "usage_record_count",
    "billing_record_count",
    "reconciliation_status",
    "amount_difference",
    "amount_comparison_status"
)

# Add execution metadata
gold_reconciliation = (
    gold_reconciliation
    .withColumn("_exec_seq", F.lit(exec_seq).cast("long"))
    .withColumn("_run_id", F.lit(run_id))
    .withColumn("_processing_timestamp", F.current_timestamp())
)


# ======================================================
# 9. SAVE GOLD DELTA TABLE
# ======================================================

(
    gold_reconciliation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_usage_billing_reconciliation")
)

print("Gold reconciliation table created successfully.")


# ======================================================
# 10. DISPLAY RECONCILIATION RESULTS
# ======================================================

display(
    spark.table("gold_usage_billing_reconciliation")
    .orderBy("customer_id", "billing_period")
)


# ======================================================
# 11. RECONCILIATION SUMMARY
# ======================================================

reconciliation_summary = (
    spark.table("gold_usage_billing_reconciliation")
    .groupBy("reconciliation_status")
    .agg(
        F.count("*").alias("record_count"),
        F.sum("total_usage_amount").alias("total_usage_amount"),
        F.sum("total_billed_amount").alias("total_billed_amount")
    )
    .orderBy("reconciliation_status")
)

display(reconciliation_summary)

print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)
print("GOLD RECONCILIATION COMPLETED")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# gold_df = spark.table("gold_usage_billing_reconciliation")

# display(
#     gold_df
#     .groupBy("reconciliation_status", "amount_comparison_status")
#     .count()
#     .orderBy("reconciliation_status", "amount_comparison_status")
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# gold_df = spark.table("gold_usage_billing_reconciliation")

# print("Gold reconciliation total:", gold_df.count())

# print("Reconciliation status counts:")

# display(
#     gold_df.groupBy("reconciliation_status")
#     .count()
#     .orderBy("reconciliation_status")
# )

# print("Amount comparison counts:")

# display(
#     gold_df.groupBy("amount_comparison_status")
#     .count()
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# from pyspark.sql import functions as F

# gold_df = spark.table("gold_usage_billing_reconciliation")

# display(
#     gold_df
#     .filter(F.col("reconciliation_status") == "YY")
#     .select(
#         "customer_id",
#         "billing_period",
#         "total_usage_amount",
#         "total_billed_amount",
#         "amount_difference",
#         "amount_comparison_status"
#     )
#     .orderBy("customer_id")
#     .limit(15)
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(
#     spark.table("silver_billing_balance")
#     .select(
#         "bill_id",
#         "customer_id",
#         "billing_period",
#         "billed_amount",
#         "tax_amount",
#         "total_amount"
#     )
#     .limit(10)
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# gold_df = spark.table("gold_usage_billing_reconciliation")

# display(
#     gold_df
#     .filter(
#         (F.col("reconciliation_status") == "YY") &
#         (F.col("amount_comparison_status") == "AMOUNT_MISMATCH")
#     )
#     .select(
#         "customer_id",
#         "billing_period",
#         "total_usage_amount",
#         "total_billed_amount",
#         "amount_difference"
#     )
#     .orderBy("customer_id")
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
