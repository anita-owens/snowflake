CREATE OR REPLACE PROCEDURE cleanup_tables_proc(database_name STRING, schema_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$

def run(session, database_name, schema_name):
    keywords = ['TEMP', 'COPY', 'BACKUP']  # List of keywords to search in table names
    dropped_tables = []

    for keyword in keywords:
        # List all tables containing the keyword in the specified database and schema
        query = f"SHOW TABLES LIKE '%{keyword}%' IN {database_name}.{schema_name}"
        tables = session.sql(query).collect()

        # Drop each table containing the keyword
        for table in tables:
            table_name = table['name']
            full_table_name = f"{database_name}.{schema_name}.{table_name}"
            drop_query = f"DROP TABLE IF EXISTS {full_table_name}"
            session.sql(drop_query).collect()
            dropped_tables.append(full_table_name)

    if dropped_tables:
        return f"Dropped tables: {', '.join(dropped_tables)}"
    else:
        return "No tables to drop."

$$;

CALL cleanup_tables_proc('YOUR_DATABASE_NAME', 'YOUR_SCHEMA_NAME');
