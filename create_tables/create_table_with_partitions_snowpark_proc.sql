use schema YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME;

CREATE OR REPLACE PROCEDURE create_partition_table(
    from_table STRING,
    to_table STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, from_table, to_table):
    # Read data from the source table
    source_df = session.read.table(from_table)
    
    # Select the desired columns and apply transformations
    transformed_df = source_df.selectExpr(
        "*",
        "YEAR(CREATED_AT) AS YEAR_COL",
        "CONCAT(YEAR(CREATED_AT), '_', LPAD(MONTH(CREATED_AT), 2, '0')) AS YEAR_MONTH_COL"
    )
    
    # Write the transformed data to the target table
    transformed_df.write.saveAsTable(to_table)
    
    return "Successfuly created partition table"
$$;


CALL create_partition_table('YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME.YOUR_TABLE_NAME', 'YOUR_DATABASE_NAME.YOUR_SCHEMA_NAME.YOUR_TEMP_TABLE_NAME');
