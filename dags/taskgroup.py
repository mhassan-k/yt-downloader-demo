from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 2),
    'retries': 2,
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=10)
    }


with DAG("Nested_Task_Group_Example",default_args=args) as dag:

    start=DummyOperator(task_id="Start")
    end=DummyOperator(task_id="End")

    with TaskGroup("Simple_Task_Group",tooltip="task_group_description") as simple_task_group:
        t1=DummyOperator(task_id="Task-1")
        t2=DummyOperator(task_id="Task-2")
        t2_1=DummyOperator(task_id="Task-2.1")
        t2_2=DummyOperator(task_id="Task-2.2")
        t3=DummyOperator(task_id="Task-3")

        t2>>t2_1
        t2>>t2_2

        with TaskGroup("Nested_Task_Group",tooltip="Nested_task_group") as nested_task_group:
            t4=DummyOperator(task_id="Task-4")
            t5=DummyOperator(task_id="Task-5")
            t5_1=DummyOperator(task_id="Task-5.1")
            t5_2=DummyOperator(task_id="Task-5.2")
            t6=DummyOperator(task_id="Task-6")

            t5>>t5_1
            t5>>t5_2

#note : Don't need to define dependencies of Nested_Task_Group they are automatically taken
start>>simple_task_group>>end
