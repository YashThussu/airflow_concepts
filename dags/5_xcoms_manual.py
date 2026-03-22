from airflow.sdk import dag,task


@dag(dag_id='xcom_dag')
def xcoms_dag_auto():
    
    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        print("Extracting data.... This is the first task")
        ti.xcom_push(key="return_result",value={"data":[1,2,3,4,5]})
    
    @task.python
    def second_task(**kwargs):

        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids="first_task",key="return_result")
        fetched_data = data['data']
        transformed_data = [x*2 for x in fetched_data]

        ti.xcom_push(key="return_result",value={"transformed_data":transformed_data})
    
    @task.python
    def third_task(**kwargs):
        
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids="second_task",key="return_result")
        load_data = data["transformed_data"]
        ti.xcom_push(key="return_result",value={"loaded_data":load_data})
    
    
    #defining task dependency
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#instantiating the dag
xcoms_dag_auto()