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
# # 1. READ GOLD RECONCILIATION TABLE
# # ------------------------------------------------------

# gold_df = spark.table(
#     "gold_usage_billing_reconciliation"
# )

# # ------------------------------------------------------
# # 2. CONFIGURABLE BUSINESS RULES
# # ------------------------------------------------------

# AMOUNT_TOLERANCE = 0.01

# # ------------------------------------------------------
# # 3. APPLY KRI CLASSIFICATION RULES
# # ------------------------------------------------------

# classified_df = (
#     gold_df
#     .withColumn(
#         "kri_id",
#         F.when(
#             F.col("reconciliation_status") == "YN",
#             "KRI_001"
#         )
#         .when(
#             F.col("reconciliation_status") == "NY",
#             "KRI_002"
#         )
#         .when(
#             (F.col("reconciliation_status") == "YY") &
#             (F.abs(F.col("amount_difference")) > AMOUNT_TOLERANCE),
#             "KRI_003"
#         )
#     )
#     .withColumn(
#         "kri_description",
#         F.when(
#             F.col("kri_id") == "KRI_001",
#             "Usage recorded but billing record missing"
#         )
#         .when(
#             F.col("kri_id") == "KRI_002",
#             "Billing record exists but usage record missing"
#         )
#         .when(
#             F.col("kri_id") == "KRI_003",
#             "Usage amount and billed amount mismatch"
#         )
#     )
#     .withColumn(
#         "classification",
#         F.when(
#             F.col("kri_id").isNotNull(),
#             "KRI"
#         ).otherwise("NON_KRI")
#     )
#     .withColumn(
#         "classification_reason",
#         F.when(
#             F.col("kri_id").isNotNull(),
#             F.col("kri_description")
#         ).otherwise(
#             "Usage and billed amounts match within tolerance"
#         )
#     )
#     .withColumn(
#         "classification_timestamp",
#         F.current_timestamp()
#     )
# )

# # ------------------------------------------------------
# # 4. CREATE SEPARATE KRI AND NON-KRI DATAFRAMES
# # ------------------------------------------------------

# kri_df = classified_df.filter(
#     F.col("classification") == "KRI"
# )

# non_kri_df = classified_df.filter(
#     F.col("classification") == "NON_KRI"
# )

# # ------------------------------------------------------
# # 5. SAVE KRI TABLE
# # ------------------------------------------------------

# (
#     kri_df.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable("gold_kri_records")
# )

# # ------------------------------------------------------
# # 6. SAVE NON-KRI TABLE
# # ------------------------------------------------------

# (
#     non_kri_df.write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .saveAsTable("gold_non_kri_records")
# )

# print("KRI and Non-KRI classification completed.")

# # ------------------------------------------------------
# # 7. DISPLAY CLASSIFICATION SUMMARY
# # ------------------------------------------------------

# display(
#     classified_df
#     .groupBy("classification", "kri_id", "kri_description")
#     .agg(
#         F.count("*").alias("record_count")
#     )
#     .orderBy("classification", "kri_id")
# )

# # ------------------------------------------------------
# # 8. VERIFY TOTAL COUNTS
# # ------------------------------------------------------

# print("Total Gold records:", gold_df.count())
# print("KRI records:", kri_df.count())
# print("Non-KRI records:", non_kri_df.count())


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

print("KRI CLASSIFICATION STARTED")
print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)


# ======================================================
# 2. READ GOLD RECONCILIATION TABLE
# ======================================================

gold_df = spark.table(
    "gold_usage_billing_reconciliation"
)

# Verify execution metadata exists
required_columns = ["_exec_seq", "_run_id"]

missing_columns = [
    c for c in required_columns
    if c not in gold_df.columns
]

if missing_columns:
    raise ValueError(
        f"Gold reconciliation table is missing metadata: {missing_columns}"
    )

# Verify all records belong to the current execution
mismatch_count = gold_df.filter(
    (F.col("_run_id") != run_id)
    | (F.col("_exec_seq") != exec_seq)
    | F.col("_run_id").isNull()
    | F.col("_exec_seq").isNull()
).limit(1).count()

if mismatch_count > 0:
    raise ValueError(
        "Execution metadata mismatch in gold_usage_billing_reconciliation"
    )

print("Gold reconciliation execution verified.")

total_gold_count = gold_df.count()

print("Total Gold reconciliation records:", total_gold_count)


# ======================================================
# 3. CONFIGURABLE BUSINESS RULES
# ======================================================

AMOUNT_TOLERANCE = 0.01

print("Configured Amount Tolerance:", AMOUNT_TOLERANCE)


# ======================================================
# 4. APPLY KRI CLASSIFICATION RULES
# ======================================================

classified_df = (
    gold_df
    .withColumn(
        "kri_id",
        F.when(
            F.col("reconciliation_status") == "YN",
            "KRI_001"
        )
        .when(
            F.col("reconciliation_status") == "NY",
            "KRI_002"
        )
        .when(
            (F.col("reconciliation_status") == "YY") &
            (F.abs(F.col("amount_difference")) > AMOUNT_TOLERANCE),
            "KRI_003"
        )
    )
    .withColumn(
        "kri_description",
        F.when(
            F.col("kri_id") == "KRI_001",
            "Usage recorded but billing record missing"
        )
        .when(
            F.col("kri_id") == "KRI_002",
            "Billing record exists but usage record missing"
        )
        .when(
            F.col("kri_id") == "KRI_003",
            "Usage amount and billed amount mismatch"
        )
    )
    .withColumn(
        "classification",
        F.when(
            F.col("kri_id").isNotNull(),
            "KRI"
        ).otherwise("NON_KRI")
    )
    .withColumn(
        "classification_reason",
        F.when(
            F.col("kri_id").isNotNull(),
            F.col("kri_description")
        ).otherwise(
            "Usage and billed amounts match within tolerance"
        )
    )
    .withColumn(
        "classification_timestamp",
        F.current_timestamp()
    )
)


# ======================================================
# 5. CREATE SEPARATE KRI AND NON-KRI DATAFRAMES
# ======================================================

kri_df = classified_df.filter(
    F.col("classification") == "KRI"
)

non_kri_df = classified_df.filter(
    F.col("classification") == "NON_KRI"
)


# ======================================================
# 6. VERIFY CLASSIFICATION COUNTS
# ======================================================

kri_count = kri_df.count()
non_kri_count = non_kri_df.count()

classified_count = kri_count + non_kri_count

if classified_count != total_gold_count:
    raise ValueError(
        f"Classification count mismatch. "
        f"Gold: {total_gold_count}, "
        f"Classified: {classified_count}"
    )

print("Classification count validation successful.")


# ======================================================
# 7. SAVE KRI TABLE
# ======================================================

(
    kri_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_kri_records")
)

print("gold_kri_records saved successfully.")


# ======================================================
# 8. SAVE NON-KRI TABLE
# ======================================================

(
    non_kri_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_non_kri_records")
)

print("gold_non_kri_records saved successfully.")


# ======================================================
# 9. DISPLAY CLASSIFICATION SUMMARY
# ======================================================

display(
    classified_df
    .groupBy(
        "classification",
        "kri_id",
        "kri_description"
    )
    .agg(
        F.count("*").alias("record_count")
    )
    .orderBy("classification", "kri_id")
)


# ======================================================
# 10. VERIFY FINAL TABLE COUNTS
# ======================================================

saved_kri_count = spark.table(
    "gold_kri_records"
).count()

saved_non_kri_count = spark.table(
    "gold_non_kri_records"
).count()

print("-----------------------------------")
print("EXECUTION SUMMARY")
print("-----------------------------------")

print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)
print("Total Gold records:", total_gold_count)
print("KRI records:", saved_kri_count)
print("Non-KRI records:", saved_non_kri_count)
print(
    "Total Classified Records:",
    saved_kri_count + saved_non_kri_count
)

if (
    saved_kri_count != kri_count
    or saved_non_kri_count != non_kri_count
):
    raise ValueError("Saved KRI/Non-KRI count validation failed.")

print("KRI and Non-KRI classification completed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
