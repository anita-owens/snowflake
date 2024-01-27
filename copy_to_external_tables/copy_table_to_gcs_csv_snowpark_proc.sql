#Step 2
USE SCHEMA YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME;

CREATE OR REPLACE PROCEDURE copy_table_to_gcs_csv(
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
        # Construct the COPY INTO statement with dynamic partition column
        copy_sql = f"COPY INTO {storage_path} FROM {from_table} PARTITION BY {partition_column} FILE_FORMAT = (TYPE = CSV);"

        # Execute the SQL statement
        session.sql(copy_sql).collect()

        return "Copy successful"
    except Exception as e:
        return f"Failed: {str(e)}"

$$;

CALL copy_table_to_gcs_csv(
    'YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME.YOUR_TABLE_NAME',
    '@gcs.stages.sf_storage/your_subfolder_if_needed/',
    'YOUR_PARTITION_COLUMN_NAME'
);
