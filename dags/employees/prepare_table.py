from airflow.providers.postgres.operators.postgres import PostgresOperator



create_employees_table_query = """
    CREATE TABLE IF NOT EXISTS tutorial.employees (
        "Serial Number" NUMERIC PRIMARY KEY,
        "Company Name" TEXT,
        "Employee Markme" TEXT,
        "Description" TEXT,
        "Leave" INTEGER
    );
"""
create_employees_table = PostgresOperator(
    task_id='create_employees_table',
    postgres_conn_id='tutorial_pg_conn',    
    sql=create_employees_table_query
)


create_employees_temp_table_query = """
    DROP TABLE IF EXISTS tutorial.employees_temp;
    CREATE TABLE tutorial.employees_temp (
        "Serial Number" NUMERIC PRIMARY KEY,
        "Company Name" TEXT,
        "Employee Markme" TEXT,
        "Description" TEXT,
        "Leave" INTEGER
    );
"""
create_employees_temp_table = PostgresOperator(
    task_id='create_employees_temp_table',
    postgres_conn_id='tutorial_pg_conn',
    sql=create_employees_temp_table_query
)
