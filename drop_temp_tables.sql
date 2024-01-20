use schema YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME;

CREATE OR REPLACE PROCEDURE drop_temp_tables(database_name STRING, schema_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$

def run(session, database_name, schema_name):
    # List tables ending with 'TEMP' in the specified database and schema
    query = f"SHOW TABLES LIKE '%TEMP' IN {database_name}.{schema_name}"
    temp_tables = session.sql(query).collect()

    # Drop each TEMP table
    for table in temp_tables:
        table_name = table['name']
        full_table_name = f"{database_name}.{schema_name}.{table_name}"
        drop_query = f"DROP TABLE IF EXISTS {full_table_name}"
        session.sql(drop_query).collect()
        #print(f"Dropped table: {full_table_name}")

    return "All TEMP tables dropped successfully."

$$;


CALL drop_temp_tables('YOUR_DATABASE_NAME', 'YOUR_SCHEMA_NAME');