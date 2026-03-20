from airflow.sdk import dag,task
from airflow.operators.bash import BashOperator



@dag(dag_id='operators_dag')
def operators_dag():
    
    @task.python
    def first_task():
        print("This is the first task")
    
    @task.python
    def second_task():
        print("This is the second task")
    
    
    @task.python
    def third_task():
        print("This is the third task")
    
    
    @task.bash
    def bash_task():
        return "echo https://airflow.apache.org/"

    
    bash_old_school = BashOperator(
        task_id = "run_after_loop",
        bash_command = "echo https://airflow.apache.org/"
    )     
    
    #defining task dependency
    first = first_task()
    second = second_task()
    third = third_task()
    bash_modern = bash_task()
    bash_oldschool = bash_old_school
    
    first >> second >> third >> bash_modern >> bash_oldschool

#instantiating the dag
operators_dag()