use schema YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME;

CREATE OR REPLACE PROCEDURE copy_table_to_gcs(
    from_table STRING,
    storage_path STRING,
    partition_column STRING,
    file_format STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$

def run(session, from_table, storage_path, partition_column, file_format):
    try:
        # Construct the COPY INTO statement with dynamic storage path, partition column, and file format
        copy_sql = f"COPY INTO {storage_path} FROM {from_table} PARTITION BY {partition_column} FILE_FORMAT = {file_format};"

        # Execute the SQL statement
        session.sql(copy_sql).collect()

        return "Copy successful"
    except Exception as e:
        # Consider logging the exception details for troubleshooting
        return f"Failed: {str(e)}"

$$;

CALL copy_table_to_gcs(
    'YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME.YOUR_TABLE_NAME',
    '@gcs.stages.sf_storage/your_subfolder_if_needed/',
    'YOUR_PARTITION_COLUMN_NAME',
    '(TYPE = CSV)'  -- Example format, change if needed
);

/*
File formats

Delimited files (CSV, TSV, etc.)

Any valid delimiter is supported; default is comma (i.e. CSV).

JSON

Parquet
*/