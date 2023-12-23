import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="turtorial",
    default_args={
        'depends_on_past': False,
        'email': ['ntloic3tbt@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='A simple tutorial DAG',
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    tags=['example']
) as dag:
    task_1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )
    
    task_2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3
    )
    
    task_1.doc_md = textwrap.dedent(
        """#### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
        **Image Credit:** Randall Munroe, [XKCD](https://xkcd.com/license.html)
        """
    )
    
    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = textwrap.dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )
    
    task_3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command
    )
    
    task_1 >> [task_2, task_3]
    
    
    