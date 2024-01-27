#Step 1
USE SCHEMA YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME;

CREATE OR REPLACE PROCEDURE add_partition_column(
    table_name STRING,
    new_partition_col_name STRING,
    date_col_name STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$

def run(session, table_name, new_partition_col_name, date_col_name):
    try:
        # Add a new partition column if it does not exist
        alter_table_sql = f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {new_partition_col_name} VARCHAR DEFAULT '';"
        session.sql(alter_table_sql).collect()

        # Update the new partition column with the year from the date column
        update_table_sql = f"UPDATE {table_name} SET {new_partition_col_name} = EXTRACT(YEAR FROM {date_col_name});"
        session.sql(update_table_sql).collect()

        return "Partition column added and updated successfully"
    except Exception as e:
        return f"Failed: {str(e)}"

$$;

CALL add_partition_column('DATABASE.SCHEMA.TABLE', 'NEW_PARTITION_COL_NAME', 'DATE_COLUMN_TO_PARTITION');
