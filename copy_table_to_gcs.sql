use schema YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME;

CREATE OR REPLACE PROCEDURE copy_table_to_gcs(from_table STRING, storage_path STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$

def run(session, from_table, storage_path):
    try:
        # Construct the COPY INTO statement
        copy_sql = f"COPY INTO {storage_path} FROM {from_table} PARTITION BY YEAR_COL FILE_FORMAT = (TYPE = CSV);"

        # Execute the SQL statement
        session.sql(copy_sql).collect()

        return "Copy successful"
    except Exception as e:
        return f"Failed: {str(e)}"

$$;

# storage_path is the external staging table connected to Google Cloud Storage
CALL copy_table_to_gcs('YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME.YOUR_TABLE_NAME', '@gcs.stages.sf_storage/your_subfolder/');