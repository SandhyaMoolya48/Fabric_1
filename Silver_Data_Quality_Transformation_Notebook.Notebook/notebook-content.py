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


from pyspark.sql import functions as F

# Step 1: Read data from Bronze
df_customers = spark.table("bronze_customers")

# Step 2: Get the original source count
source_count = df_customers.count()

# Step 3: Identify exact duplicate records
df_customers_unique = df_customers.dropDuplicates()

# Step 4: Calculate duplicate count
unique_count = df_customers_unique.count()

duplicate_count = source_count - unique_count

# Step 5: Display results
print("CUSTOMER DATA DUPLICATE CHECK")
print("--------------------------------")
print(f"Bronze Source Count : {source_count}")
print(f"Unique Record Count : {unique_count}")
print(f"Duplicate Count     : {duplicate_count}")

# Step 6: Display unique records
df_customers_unique.show(10, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

bronze_tables = [
    "bronze_customers",
    "bronze_usage",
    "bronze_billing",
    "bronze_payments",
    "bronze_subscriptions",
    "bronze_network_events"
]

duplicate_summary = []

for table_name in bronze_tables:

    df = spark.table(table_name)

    source_count = df.count()
    unique_count = df.dropDuplicates().count()
    duplicate_count = source_count - unique_count

    duplicate_summary.append(
        (table_name, source_count, unique_count, duplicate_count)
    )

# Create summary DataFrame
duplicate_df = spark.createDataFrame(
    duplicate_summary,
    [
        "table_name",
        "source_count",
        "unique_count",
        "duplicate_count"
    ]
)

display(duplicate_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# from here start

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------
# 1. Define primary keys for each source table
# --------------------------------------------------

primary_key_config = {
    "bronze_customers": ["customer_id"],
    "bronze_usage": ["usage_id"],
    "bronze_billing": ["bill_id"],
    "bronze_payments": ["payment_id"],
    "bronze_subscriptions": ["subscription_id"],
    "bronze_network_events": ["event_id"]
}

# --------------------------------------------------
# 2. Create dictionaries to store the results
# --------------------------------------------------

silver_clean_dfs = {}
quarantine_dfs = {}
audit_results = []

# --------------------------------------------------
# 3. Process each Bronze table
# --------------------------------------------------

for table_name, primary_keys in primary_key_config.items():

    print(f"Processing: {table_name}")

    # Read Bronze Delta table
    bronze_df = spark.table(table_name)

    # Original source columns only.
    # Exclude technical ingestion metadata.
    source_columns = [
        col for col in bronze_df.columns
        if col not in [
            "_source_file",
            "_ingestion_timestamp",
            "_ingestion_date"
        ]
    ]

    # Total source count
    source_count = bronze_df.count()

    # --------------------------------------------------
    # 4. Exact duplicate detection
    # Based on ALL original source columns
    # --------------------------------------------------

    duplicate_window = (
        Window
        .partitionBy(*source_columns)
        .orderBy(F.col("_ingestion_timestamp"))
    )

    duplicate_df = (
        bronze_df
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
        duplicate_df
        .filter(F.col("_is_duplicate"))
        .count()
    )

    # --------------------------------------------------
    # 5. Primary key NULL / empty validation
    # Only primary keys are checked here
    # --------------------------------------------------

    invalid_pk_condition = None

    for pk in primary_keys:

        condition = (
            F.col(pk).isNull() |
            (F.trim(F.col(pk).cast("string")) == "")
        )

        if invalid_pk_condition is None:
            invalid_pk_condition = condition
        else:
            invalid_pk_condition = (
                invalid_pk_condition | condition
            )

    validated_df = duplicate_df.withColumn(
        "_invalid_pk",
        invalid_pk_condition
    )

    invalid_pk_count = (
        validated_df
        .filter(F.col("_invalid_pk"))
        .count()
    )

    # --------------------------------------------------
    # 6. Add rejection reason
    # --------------------------------------------------

    rejected_df = (
        validated_df
        .filter(
            F.col("_is_duplicate") |
            F.col("_invalid_pk")
        )
        .withColumn(
            "_rejection_reason",
            F.concat_ws(
                " | ",
                F.when(
                    F.col("_is_duplicate"),
                    F.lit("Exact duplicate record")
                ),
                F.when(
                    F.col("_invalid_pk"),
                    F.lit("Primary key is NULL or empty")
                )
            )
        )
        .drop("_duplicate_rank", "_is_duplicate", "_invalid_pk")
    )

    # --------------------------------------------------
    # 7. Create valid Balance DataFrame
    # --------------------------------------------------

    clean_df = (
        validated_df
        .filter(
            (~F.col("_is_duplicate")) &
            (~F.col("_invalid_pk"))
        )
        .drop("_duplicate_rank", "_is_duplicate", "_invalid_pk")
    )

    # --------------------------------------------------
    # 8. Calculate counts
    # --------------------------------------------------

    filter_count = rejected_df.count()
    balance_count = clean_df.count()

    # Store DataFrames for the next step
    silver_clean_dfs[table_name] = clean_df
    quarantine_dfs[table_name] = rejected_df

    # Audit information
    audit_results.append((
        table_name,
        source_count,
        duplicate_count,
        invalid_pk_count,
        filter_count,
        balance_count
    ))

    print(f"Source Count: {source_count}")
    print(f"Duplicate Count: {duplicate_count}")
    print(f"Invalid PK Count: {invalid_pk_count}")
    print(f"Filter Count: {filter_count}")
    print(f"Balance Count: {balance_count}")

print("Silver validation completed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# --------------------------------------------------
# Save separate Silver Balance and Filter tables
# --------------------------------------------------

for table_name in primary_key_config.keys():

    source_name = table_name.replace("bronze_", "")

    balance_table = f"silver_{source_name}_balance"
    filter_table = f"silver_{source_name}_filter"

    # Save valid records
    silver_clean_dfs[table_name] \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(balance_table)

    # Save rejected records
    quarantine_dfs[table_name] \
        .write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(filter_table)

    print(f"Created: {balance_table}")
    print(f"Created: {filter_table}")

print("All Silver Balance and Filter tables created.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# verifiaction

# CELL ********************


# Verify all Silver tables

for table_name in primary_key_config.keys():

    source_name = table_name.replace("bronze_", "")

    balance_table = f"silver_{source_name}_balance"
    filter_table = f"silver_{source_name}_filter"

    balance_count = spark.table(balance_table).count()
    filter_count = spark.table(filter_table).count()

    print(f"\nSource: {source_name}")
    print(f"Balance count: {balance_count}")
    print(f"Filter count: {filter_count}")

    display(spark.table(filter_table).limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# next step 

# CELL ********************


# Create Silver audit DataFrame

audit_columns = [
    "source_table",
    "source_count",
    "duplicate_count",
    "invalid_pk_count",
    "filter_count",
    "balance_count"
]

audit_df = spark.createDataFrame(
    audit_results,
    audit_columns
)

# Add processing timestamp
audit_df = audit_df.withColumn(
    "processed_timestamp",
    F.current_timestamp()
)

# Save audit table
audit_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_audit_summary")

display(spark.table("silver_audit_summary"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# veification

# CELL ********************


# Read the existing Silver Usage Balance table

usage_df = spark.table("silver_usage_balance")

# Display table structure
usage_df.printSchema()

# Display sample usage date and amount values
display(
    usage_df.select(
        "usage_id",
        "usage_date",
        "usage_amount"
    ).limit(20)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F

# Read existing valid usage records
usage_df = spark.table("silver_usage_balance")

# Validate date and amount without changing original columns
validated_usage_df = (
    usage_df
    .withColumn(
        "_parsed_usage_date",
        F.expr("try_to_timestamp(usage_date, 'yyyy-MM-dd')")
    )
    .withColumn(
        "_parsed_usage_amount",
        F.expr("try_cast(usage_amount AS DOUBLE)")
    )
)

# Date validation
invalid_date = (
    F.col("usage_date").isNull() |
    (F.trim(F.col("usage_date")) == "") |
    F.col("_parsed_usage_date").isNull()
)

# Amount validation
invalid_amount = (
    F.col("usage_amount").isNull() |
    (F.trim(F.col("usage_amount")) == "") |
    F.col("_parsed_usage_amount").isNull()
)

# Identify records with one or both validation failures
usage_rejected_df = (
    validated_usage_df
    .filter(invalid_date | invalid_amount)
    .withColumn(
        "_rejection_reason",
        F.concat_ws(
            " | ",
            F.when(
                invalid_date,
                F.lit("Invalid or missing usage_date")
            ),
            F.when(
                invalid_amount,
                F.lit("Invalid or missing usage_amount")
            )
        )
    )
    .drop("_parsed_usage_date", "_parsed_usage_amount")
)

# Identify valid records
usage_valid_df = (
    validated_usage_df
    .filter(~(invalid_date | invalid_amount))
    .drop("_parsed_usage_date", "_parsed_usage_amount")
)

# Display validation results
print("Current Balance Count:", usage_df.count())
print("New Invalid Records:", usage_rejected_df.count())
print("New Valid Records:", usage_valid_df.count())

print("Rejected usage records:")
display(usage_rejected_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# next step


# CELL ********************


# Read existing usage filter table
existing_filter_df = spark.table("silver_usage_filter")

# Add new rejected records to the existing filter
updated_filter_df = existing_filter_df.unionByName(
    usage_rejected_df,
    allowMissingColumns=True
)

# Save updated Filter table
updated_filter_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_usage_filter")

# Save updated Balance table
usage_valid_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_usage_balance")

print("Usage Filter and Balance tables updated successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# verifiaction

# CELL ********************


# Verify updated usage tables

final_balance = spark.table("silver_usage_balance")
final_filter = spark.table("silver_usage_filter")

print("Final Usage Balance Count:", final_balance.count())
print("Final Usage Filter Count:", final_filter.count())

print("Filter table rejection reasons:")
display(
    final_filter.groupBy("_rejection_reason")
    .count()
)

print("Updated Usage Balance:")
display(final_balance.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
