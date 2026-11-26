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

# # All six Bronze source tables
# bronze_tables = [
#     "bronze_customers",
#     "bronze_usage",
#     "bronze_billing",
#     "bronze_payments",
#     "bronze_subscriptions",
#     "bronze_network_events"
# ]

# # Inspect all schemas and column names
# for table_name in bronze_tables:

#     print("\n" + "=" * 60)
#     print(f"TABLE: {table_name}")
#     print("=" * 60)

#     df = spark.table(table_name)

#     df.printSchema()

#     print("Total records:", df.count())
#     print("Columns:", df.columns)


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


# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# # ==========================================================
# # 1. SOURCE VALIDATION CONFIGURATION
# # ==========================================================

# validation_config = {

#     "bronze_customers": {
#         "primary_key": ["customer_id"],
#         "date_columns": ["registration_date"],
#         "optional_date_columns": [],
#         "numeric_columns": []
#     },

#     "bronze_usage": {
#         "primary_key": ["usage_id"],
#         "date_columns": ["usage_date"],
#         "optional_date_columns": [],
#         "numeric_columns": ["usage_quantity", "usage_amount"]
#     },

#     "bronze_billing": {
#         "primary_key": ["bill_id"],
#         "date_columns": ["billing_date"],
#         "optional_date_columns": [],
#         "numeric_columns": [
#             "billed_amount",
#             "tax_amount",
#             "total_amount"
#         ]
#     },

#     "bronze_payments": {
#         "primary_key": ["payment_id"],
#         "date_columns": ["payment_date"],
#         "optional_date_columns": [],
#         "numeric_columns": ["payment_amount"]
#     },

#     "bronze_subscriptions": {
#         "primary_key": ["subscription_id"],
#         "date_columns": ["start_date"],
#         "optional_date_columns": ["end_date"],
#         "numeric_columns": ["monthly_charge"]
#     },

#     "bronze_network_events": {
#     "primary_key": ["event_id"],
#     "date_columns": ["event_date"],
#     "optional_date_columns": [],
#     "numeric_columns": [],
#     "date_formats": {
#         "event_date": "yyyy-MM-dd HH:mm:ss"
#     }
#     }

# }


# # ==========================================================
# # 2. INITIALIZE AUDIT RESULTS
# # ==========================================================

# audit_results = []

# # Technical metadata columns
# metadata_columns = [
#     "_source_file",
#     "_ingestion_timestamp",
#     "_ingestion_date"
# ]


# # ==========================================================
# # 3. PROCESS ALL SIX BRONZE TABLES
# # ==========================================================

# for table_name, config in validation_config.items():

#     print("\n" + "=" * 65)
#     print(f"PROCESSING TABLE: {table_name}")
#     print("=" * 65)

#     # Read original Bronze data
#     df = spark.table(table_name)

#     source_count = df.count()

#     # Original business/source columns
#     source_columns = [
#         c for c in df.columns
#         if c not in metadata_columns
#     ]

#     # Check configured columns exist
#     configured_columns = (
#         config["primary_key"]
#         + config["date_columns"]
#         + config["optional_date_columns"]
#         + config["numeric_columns"]
#     )

#     missing_columns = [
#         c for c in configured_columns
#         if c not in df.columns
#     ]

#     if missing_columns:
#         raise ValueError(
#             f"{table_name} is missing columns: {missing_columns}"
#         )

#     # ------------------------------------------------------
#     # 4. EXACT DUPLICATE VALIDATION
#     # Compare all original source columns
#     # ------------------------------------------------------

#     duplicate_window = (
#         Window
#         .partitionBy(*source_columns)
#         .orderBy(F.col("_ingestion_timestamp"))
#     )

#     df = (
#         df
#         .withColumn(
#             "_duplicate_rank",
#             F.row_number().over(duplicate_window)
#         )
#         .withColumn(
#             "_is_duplicate",
#             F.col("_duplicate_rank") > 1
#         )
#     )

#     duplicate_count = (
#         df.filter(F.col("_is_duplicate")).count()
#     )

#     # ------------------------------------------------------
#     # 5. PRIMARY KEY NULL / EMPTY VALIDATION
#     # ------------------------------------------------------

#     invalid_pk_condition = None

#     for pk in config["primary_key"]:

#         condition = (
#             F.col(pk).isNull()
#             | (F.trim(F.col(pk).cast("string")) == "")
#         )

#         if invalid_pk_condition is None:
#             invalid_pk_condition = condition
#         else:
#             invalid_pk_condition = (
#                 invalid_pk_condition | condition
#             )

#     df = df.withColumn(
#         "_invalid_pk",
#         invalid_pk_condition
#     )

#     invalid_pk_count = (
#         df.filter(F.col("_invalid_pk")).count()
#     )

    

#     # ------------------------------------------------------
#     # 6. DATE VALIDATION
#     # Supports source-specific date formats
#     # Default format: yyyy-MM-dd
#     # ------------------------------------------------------

#     invalid_date_condition = None
#     date_reasons = []

#     # Required date columns
#     for date_col in config["date_columns"]:

#         parsed_col = f"_parsed_{date_col}"

#         # Use source-specific format if configured;
#         # otherwise use yyyy-MM-dd
#         date_format = config.get("date_formats", {}).get(
#             date_col, "yyyy-MM-dd"
#         )

#         df = df.withColumn(
#             parsed_col,
#             F.expr(
#                 f"try_to_timestamp(`{date_col}`, '{date_format}')"
#             )
#         )

#         condition = (
#             F.col(date_col).isNull()
#             | (F.trim(F.col(date_col)) == "")
#             | F.col(parsed_col).isNull()
#         )

#         if invalid_date_condition is None:
#             invalid_date_condition = condition
#         else:
#             invalid_date_condition = (
#                 invalid_date_condition | condition
#             )

#         date_reasons.append(
#             F.when(
#                 condition,
#                 F.lit(f"Invalid or missing {date_col}")
#             )
#         )

#     # Optional date columns
#     # NULL/empty is allowed, but invalid populated values are rejected
#     for date_col in config["optional_date_columns"]:

#         parsed_col = f"_parsed_{date_col}"

#         date_format = config.get("date_formats", {}).get(
#             date_col, "yyyy-MM-dd"
#         )

#         df = df.withColumn(
#             parsed_col,
#             F.expr(
#                 f"try_to_timestamp(`{date_col}`, '{date_format}')"
#             )
#         )

#         condition = (
#             F.col(date_col).isNotNull()
#             & (F.trim(F.col(date_col)) != "")
#             & F.col(parsed_col).isNull()
#         )

#         if invalid_date_condition is None:
#             invalid_date_condition = condition
#         else:
#             invalid_date_condition = (
#                 invalid_date_condition | condition
#             )

#         date_reasons.append(
#             F.when(
#                 condition,
#                 F.lit(f"Invalid {date_col}")
#             )
#         )

#     if invalid_date_condition is None:
#         invalid_date_condition = F.lit(False)

#     df = df.withColumn(
#         "_invalid_date",
#         invalid_date_condition
#     )

#     invalid_date_count = (
#         df.filter(F.col("_invalid_date")).count()
#     )

#     # ------------------------------------------------------
#     # 7. NUMERIC VALIDATION
#     # ------------------------------------------------------

#     invalid_numeric_condition = None
#     numeric_reasons = []

#     for numeric_col in config["numeric_columns"]:

#         parsed_col = f"_parsed_{numeric_col}"

#         df = df.withColumn(
#             parsed_col,
#             F.expr(
#                 f"try_cast(`{numeric_col}` AS DOUBLE)"
#             )
#         )

#         condition = (
#             F.col(numeric_col).isNull()
#             | (F.trim(F.col(numeric_col)) == "")
#             | F.col(parsed_col).isNull()
#         )

#         if invalid_numeric_condition is None:
#             invalid_numeric_condition = condition
#         else:
#             invalid_numeric_condition = (
#                 invalid_numeric_condition | condition
#             )

#         numeric_reasons.append(
#             F.when(
#                 condition,
#                 F.lit(f"Invalid or missing {numeric_col}")
#             )
#         )

#     if invalid_numeric_condition is None:
#         invalid_numeric_condition = F.lit(False)

#     df = df.withColumn(
#         "_invalid_numeric",
#         invalid_numeric_condition
#     )

#     invalid_numeric_count = (
#         df.filter(F.col("_invalid_numeric")).count()
#     )

#     # ------------------------------------------------------
#     # 8. COMBINE ALL REJECTION CONDITIONS
#     # ------------------------------------------------------

#     rejection_condition = (
#         F.col("_is_duplicate")
#         | F.col("_invalid_pk")
#         | F.col("_invalid_date")
#         | F.col("_invalid_numeric")
#     )

#     # Build rejection descriptions
#     rejection_reasons = [
#         F.when(
#             F.col("_is_duplicate"),
#             F.lit("Exact duplicate record")
#         ),
#         F.when(
#             F.col("_invalid_pk"),
#             F.lit("Primary key is NULL or empty")
#         )
#     ]

#     rejection_reasons.extend(date_reasons)
#     rejection_reasons.extend(numeric_reasons)

#     # ------------------------------------------------------
#     # 9. CREATE FILTER DATAFRAME
#     # ------------------------------------------------------

#     filter_df = (
#         df
#         .filter(rejection_condition)
#         .withColumn(
#             "_rejection_reason",
#             F.concat_ws(" | ", *rejection_reasons)
#         )
#         .drop(
#             "_duplicate_rank",
#             "_is_duplicate",
#             "_invalid_pk",
#             "_invalid_date",
#             "_invalid_numeric"
#         )
#     )

#     # ------------------------------------------------------
#     # 10. CREATE BALANCE DATAFRAME
#     # ------------------------------------------------------

#     balance_df = (
#         df
#         .filter(~rejection_condition)
#         .drop(
#             "_duplicate_rank",
#             "_is_duplicate",
#             "_invalid_pk",
#             "_invalid_date",
#             "_invalid_numeric"
#         )
#     )

#     # Remove temporary parsed columns
#     temporary_columns = [
#         c for c in balance_df.columns
#         if c.startswith("_parsed_")
#     ]

#     balance_df = balance_df.drop(*temporary_columns)

#     temporary_filter_columns = [
#         c for c in filter_df.columns
#         if c.startswith("_parsed_")
#     ]

#     filter_df = filter_df.drop(*temporary_filter_columns)

#     # ------------------------------------------------------
#     # 11. CALCULATE FINAL COUNTS
#     # ------------------------------------------------------

#     filter_count = filter_df.count()
#     balance_count = balance_df.count()

#     # ------------------------------------------------------
#     # 12. SAVE BALANCE AND FILTER TABLES
#     # ------------------------------------------------------

#     source_name = table_name.replace("bronze_", "")

#     balance_table = f"silver_{source_name}_balance"
#     filter_table = f"silver_{source_name}_filter"

#     balance_df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("overwriteSchema", "true") \
#         .saveAsTable(balance_table)

#     filter_df.write \
#         .format("delta") \
#         .mode("overwrite") \
#         .option("overwriteSchema", "true") \
#         .saveAsTable(filter_table)

#     # ------------------------------------------------------
#     # 13. COLLECT AUDIT INFORMATION
#     # ------------------------------------------------------

#     audit_results.append((
#         table_name,
#         source_count,
#         duplicate_count,
#         invalid_pk_count,
#         invalid_date_count,
#         invalid_numeric_count,
#         filter_count,
#         balance_count,
#         "SUCCESS"
#     ))

#     print(f"Source Count       : {source_count}")
#     print(f"Duplicate Count    : {duplicate_count}")
#     print(f"Invalid PK Count   : {invalid_pk_count}")
#     print(f"Invalid Date Count : {invalid_date_count}")
#     print(f"Invalid Numeric    : {invalid_numeric_count}")
#     print(f"Filter Count       : {filter_count}")
#     print(f"Balance Count      : {balance_count}")

#     print(f"Saved: {balance_table}")
#     print(f"Saved: {filter_table}")

#     # Count reconciliation check
#     if source_count != balance_count + filter_count:
#         raise ValueError(
#             f"Count mismatch in {table_name}"
#         )


# # ==========================================================
# # 14. CREATE CENTRAL SILVER AUDIT SUMMARY
# # ==========================================================

# audit_columns = [
#     "source_table",
#     "source_count",
#     "duplicate_count",
#     "invalid_pk_count",
#     "invalid_date_count",
#     "invalid_numeric_count",
#     "filter_count",
#     "balance_count",
#     "processing_status"
# ]

# audit_df = spark.createDataFrame(
#     audit_results,
#     audit_columns
# )

# audit_df = audit_df.withColumn(
#     "processed_timestamp",
#     F.current_timestamp()
# )

# audit_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable("silver_audit_summary")


# # ==========================================================
# # 15. DISPLAY FINAL AUDIT
# # ==========================================================

# print("\nSILVER DATA QUALITY FRAMEWORK COMPLETED")

# display(
#     spark.table("silver_audit_summary")
#     .orderBy("source_table")
# )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(
#     spark.table("bronze_network_events")
#     .select("event_id", "event_date")
#     .show(20, truncate=False)
# )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==========================================================
# 1. EXECUTION VALIDATION
# ==========================================================

if not exec_seq or not run_id:
    raise ValueError(
        "exec_seq and run_id must be provided by the orchestrator."
    )

exec_seq = int(exec_seq)
run_id = str(run_id)

print("SILVER DATA QUALITY FRAMEWORK STARTED")
print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)


# ==========================================================
# 2. SOURCE VALIDATION CONFIGURATION
# ==========================================================

validation_config = {

    "bronze_customers": {
        "primary_key": ["customer_id"],
        "date_columns": ["registration_date"],
        "optional_date_columns": [],
        "numeric_columns": []
    },

    "bronze_usage": {
        "primary_key": ["usage_id"],
        "date_columns": ["usage_date"],
        "optional_date_columns": [],
        "numeric_columns": ["usage_quantity", "usage_amount"]
    },

    "bronze_billing": {
        "primary_key": ["bill_id"],
        "date_columns": ["billing_date"],
        "optional_date_columns": [],
        "numeric_columns": [
            "billed_amount",
            "tax_amount",
            "total_amount"
        ]
    },

    "bronze_payments": {
        "primary_key": ["payment_id"],
        "date_columns": ["payment_date"],
        "optional_date_columns": [],
        "numeric_columns": ["payment_amount"]
    },

    "bronze_subscriptions": {
        "primary_key": ["subscription_id"],
        "date_columns": ["start_date"],
        "optional_date_columns": ["end_date"],
        "numeric_columns": ["monthly_charge"]
    },

    "bronze_network_events": {
        "primary_key": ["event_id"],
        "date_columns": ["event_date"],
        "optional_date_columns": [],
        "numeric_columns": [],
        "date_formats": {
            "event_date": "yyyy-MM-dd HH:mm:ss"
        }
    }
}


# ==========================================================
# 3. INITIALIZE AUDIT RESULTS
# ==========================================================

audit_results = []

# All technical metadata columns
# These columns are excluded from duplicate comparison.

metadata_columns = [
    "_source_file",
    "_ingestion_timestamp",
    "_ingestion_date",
    "_run_id",
    "_exec_seq"
]


# ==========================================================
# 4. PROCESS ALL SIX BRONZE TABLES
# ==========================================================

for table_name, config in validation_config.items():

    print("\n" + "=" * 65)
    print(f"PROCESSING TABLE: {table_name}")
    print("=" * 65)

    # Read Bronze data
    df = spark.table(table_name)

    source_count = df.count()

    # ------------------------------------------------------
    # 4.1 VERIFY EXECUTION METADATA
    # ------------------------------------------------------

    required_metadata = ["_run_id", "_exec_seq"]

    missing_metadata = [
        c for c in required_metadata
        if c not in df.columns
    ]

    if missing_metadata:
        raise ValueError(
            f"{table_name} is missing execution metadata: "
            f"{missing_metadata}"
        )

    # Verify all rows belong to the current execution
    mismatched_rows = df.filter(
        (F.col("_run_id") != run_id)
        | (F.col("_exec_seq") != exec_seq)
        | F.col("_run_id").isNull()
        | F.col("_exec_seq").isNull()
    ).limit(1).count()

    if mismatched_rows > 0:
        raise ValueError(
            f"Execution metadata mismatch in {table_name}. "
            f"Expected exec_seq={exec_seq}, run_id={run_id}"
        )

    print("Execution metadata verified successfully")

    # Original business/source columns
    source_columns = [
        c for c in df.columns
        if c not in metadata_columns
    ]

    # ------------------------------------------------------
    # 4.2 CHECK CONFIGURED COLUMNS
    # ------------------------------------------------------

    configured_columns = (
        config["primary_key"]
        + config["date_columns"]
        + config["optional_date_columns"]
        + config["numeric_columns"]
    )

    missing_columns = [
        c for c in configured_columns
        if c not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing columns: {missing_columns}"
        )

    # ------------------------------------------------------
    # 5. EXACT DUPLICATE VALIDATION
    # Compare all original source columns
    # ------------------------------------------------------

    duplicate_window = (
        Window
        .partitionBy(*source_columns)
        .orderBy(F.col("_ingestion_timestamp"))
    )

    df = (
        df
        .withColumn(
            "_duplicate_rank",
            F.row_number().over(duplicate_window)
        )
        .withColumn(
            "_is_duplicate",
            F.col("_duplicate_rank") > 1
        )
    )

    duplicate_count = (
        df.filter(F.col("_is_duplicate")).count()
    )

    # ------------------------------------------------------
    # 6. PRIMARY KEY NULL / EMPTY VALIDATION
    # ------------------------------------------------------

    invalid_pk_condition = None

    for pk in config["primary_key"]:

        condition = (
            F.col(pk).isNull()
            | (F.trim(F.col(pk).cast("string")) == "")
        )

        if invalid_pk_condition is None:
            invalid_pk_condition = condition
        else:
            invalid_pk_condition = (
                invalid_pk_condition | condition
            )

    df = df.withColumn(
        "_invalid_pk",
        invalid_pk_condition
    )

    invalid_pk_count = (
        df.filter(F.col("_invalid_pk")).count()
    )

    # ------------------------------------------------------
    # 7. DATE VALIDATION
    # Default format: yyyy-MM-dd
    # Supports source-specific formats
    # ------------------------------------------------------

    invalid_date_condition = None
    date_reasons = []

    # Required date columns
    for date_col in config["date_columns"]:

        parsed_col = f"_parsed_{date_col}"

        date_format = config.get(
            "date_formats", {}
        ).get(date_col, "yyyy-MM-dd")

        df = df.withColumn(
            parsed_col,
            F.expr(
                f"try_to_timestamp(`{date_col}`, '{date_format}')"
            )
        )

        condition = (
            F.col(date_col).isNull()
            | (F.trim(F.col(date_col)) == "")
            | F.col(parsed_col).isNull()
        )

        if invalid_date_condition is None:
            invalid_date_condition = condition
        else:
            invalid_date_condition = (
                invalid_date_condition | condition
            )

        date_reasons.append(
            F.when(
                condition,
                F.lit(f"Invalid or missing {date_col}")
            )
        )

    # Optional date columns
    # NULL/empty is allowed, but invalid populated values
    # are rejected.
    for date_col in config["optional_date_columns"]:

        parsed_col = f"_parsed_{date_col}"

        date_format = config.get(
            "date_formats", {}
        ).get(date_col, "yyyy-MM-dd")

        df = df.withColumn(
            parsed_col,
            F.expr(
                f"try_to_timestamp(`{date_col}`, '{date_format}')"
            )
        )

        condition = (
            F.col(date_col).isNotNull()
            & (F.trim(F.col(date_col)) != "")
            & F.col(parsed_col).isNull()
        )

        if invalid_date_condition is None:
            invalid_date_condition = condition
        else:
            invalid_date_condition = (
                invalid_date_condition | condition
            )

        date_reasons.append(
            F.when(
                condition,
                F.lit(f"Invalid {date_col}")
            )
        )

    if invalid_date_condition is None:
        invalid_date_condition = F.lit(False)

    df = df.withColumn(
        "_invalid_date",
        invalid_date_condition
    )

    invalid_date_count = (
        df.filter(F.col("_invalid_date")).count()
    )

    # ------------------------------------------------------
    # 8. NUMERIC VALIDATION
    # ------------------------------------------------------

    invalid_numeric_condition = None
    numeric_reasons = []

    for numeric_col in config["numeric_columns"]:

        parsed_col = f"_parsed_{numeric_col}"

        df = df.withColumn(
            parsed_col,
            F.expr(
                f"try_cast(`{numeric_col}` AS DOUBLE)"
            )
        )

        condition = (
            F.col(numeric_col).isNull()
            | (F.trim(F.col(numeric_col)) == "")
            | F.col(parsed_col).isNull()
        )

        if invalid_numeric_condition is None:
            invalid_numeric_condition = condition
        else:
            invalid_numeric_condition = (
                invalid_numeric_condition | condition
            )

        numeric_reasons.append(
            F.when(
                condition,
                F.lit(f"Invalid or missing {numeric_col}")
            )
        )

    if invalid_numeric_condition is None:
        invalid_numeric_condition = F.lit(False)

    df = df.withColumn(
        "_invalid_numeric",
        invalid_numeric_condition
    )

    invalid_numeric_count = (
        df.filter(F.col("_invalid_numeric")).count()
    )

    # ------------------------------------------------------
    # 9. COMBINE ALL REJECTION CONDITIONS
    # ------------------------------------------------------

    rejection_condition = (
        F.col("_is_duplicate")
        | F.col("_invalid_pk")
        | F.col("_invalid_date")
        | F.col("_invalid_numeric")
    )

    # Build rejection descriptions
    rejection_reasons = [
        F.when(
            F.col("_is_duplicate"),
            F.lit("Exact duplicate record")
        ),
        F.when(
            F.col("_invalid_pk"),
            F.lit("Primary key is NULL or empty")
        )
    ]

    rejection_reasons.extend(date_reasons)
    rejection_reasons.extend(numeric_reasons)

    # ------------------------------------------------------
    # 10. CREATE FILTER DATAFRAME
    # ------------------------------------------------------

    filter_df = (
        df
        .filter(rejection_condition)
        .withColumn(
            "_rejection_reason",
            F.concat_ws(" | ", *rejection_reasons)
        )
        .drop(
            "_duplicate_rank",
            "_is_duplicate",
            "_invalid_pk",
            "_invalid_date",
            "_invalid_numeric"
        )
    )

    # ------------------------------------------------------
    # 11. CREATE BALANCE DATAFRAME
    # ------------------------------------------------------

    balance_df = (
        df
        .filter(~rejection_condition)
        .drop(
            "_duplicate_rank",
            "_is_duplicate",
            "_invalid_pk",
            "_invalid_date",
            "_invalid_numeric"
        )
    )

    # Remove temporary parsed columns
    temporary_columns = [
        c for c in balance_df.columns
        if c.startswith("_parsed_")
    ]

    balance_df = balance_df.drop(*temporary_columns)

    temporary_filter_columns = [
        c for c in filter_df.columns
        if c.startswith("_parsed_")
    ]

    filter_df = filter_df.drop(*temporary_filter_columns)

    # ------------------------------------------------------
    # 12. CALCULATE FINAL COUNTS
    # ------------------------------------------------------

    filter_count = filter_df.count()
    balance_count = balance_df.count()

    # ------------------------------------------------------
    # 13. SAVE BALANCE AND FILTER TABLES
    # ------------------------------------------------------

    source_name = table_name.replace("bronze_", "")

    balance_table = f"silver_{source_name}_balance"
    filter_table = f"silver_{source_name}_filter"

    balance_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(balance_table)

    filter_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(filter_table)

    # ------------------------------------------------------
    # 14. COLLECT AUDIT INFORMATION
    # ------------------------------------------------------

    audit_results.append((
        exec_seq,
        run_id,
        table_name,
        source_count,
        duplicate_count,
        invalid_pk_count,
        invalid_date_count,
        invalid_numeric_count,
        filter_count,
        balance_count,
        "SUCCESS"
    ))

    print(f"Source Count       : {source_count}")
    print(f"Duplicate Count    : {duplicate_count}")
    print(f"Invalid PK Count   : {invalid_pk_count}")
    print(f"Invalid Date Count : {invalid_date_count}")
    print(f"Invalid Numeric    : {invalid_numeric_count}")
    print(f"Filter Count       : {filter_count}")
    print(f"Balance Count      : {balance_count}")

    print(f"Saved: {balance_table}")
    print(f"Saved: {filter_table}")

    # Count reconciliation check
    if source_count != balance_count + filter_count:
        raise ValueError(
            f"Count mismatch in {table_name}"
        )


# ==========================================================
# 15. CREATE CENTRAL SILVER AUDIT SUMMARY
# ==========================================================

audit_columns = [
    "exec_seq",
    "run_id",
    "source_table",
    "source_count",
    "duplicate_count",
    "invalid_pk_count",
    "invalid_date_count",
    "invalid_numeric_count",
    "filter_count",
    "balance_count",
    "processing_status"
]

audit_df = spark.createDataFrame(
    audit_results,
    audit_columns
)

audit_df = audit_df.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)

audit_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_audit_summary")


# ==========================================================
# 16. DISPLAY FINAL AUDIT
# ==========================================================

print("\nSILVER DATA QUALITY FRAMEWORK COMPLETED")

print("Execution Sequence:", exec_seq)
print("Run ID:", run_id)

display(
    spark.table("silver_audit_summary")
    .orderBy("source_table")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
