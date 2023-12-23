import datetime

import pendulum
from airflow.decorators import dag

from dags.employees.prepare_table import create_employees_table, create_employees_temp_table
from dags.employees.tasks import get_data, merge_data

@dag(
    dag_id='process-employees',
    schedule='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz='UTC'),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=['tutorial'],
)
def process_employees():
    [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()
    
process_employees()