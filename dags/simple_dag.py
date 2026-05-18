import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_hello_python():
    """A simple python function to be called by a PythonOperator."""
    print("Hello from the Python Operator!")

# Define the DAG
with DAG(
    dag_id="simple_example_dag",
    # This defines the schedule interval (at midnight every day)
    schedule="0 0 * * *",
    # The start_date is the date the DAG will start being scheduled
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example"],
) as dag:

    # Task 1: Execute a shell command
    task_bash = BashOperator(
        task_id="run_bash_command",
        bash_command="echo 'Hello from the Bash Operator!'",
    )

    # Task 2: Execute a Python function
    task_python = PythonOperator(
        task_id="run_python_function",
        python_callable=print_hello_python,
    )

    # Set task dependencies: task_bash runs first, then task_python
    task_bash >> task_python
