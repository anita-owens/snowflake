CREATE OR REPLACE PROCEDURE copy_partitioned_table_to_gcs_bucket_csv_format(
    from_table STRING,
    storage_path STRING,
    partition_column STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$

def run(session, from_table, storage_path, partition_column):
    try:
        # Construct the COPY INTO statement with dynamic partition column and hardcoded storage integration name. In this case ‘gcs_int’ from Snowflake documentation
        copy_sql = f"COPY INTO '{storage_path}' FROM {from_table} PARTITION BY {partition_column} FILE_FORMAT = (TYPE = CSV, NULL_IF = ('NULL', 'null'), EMPTY_FIELD_AS_NULL = false) HEADER = TRUE STORAGE_INTEGRATION = gcs_int;"

        # Execute the SQL statement
        session.sql(copy_sql).collect()

        return "Copy successful"
    except Exception as e:
        return f"Failed: {str(e)}"

$$;
