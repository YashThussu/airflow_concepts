from airflow.sdk import dag,task


@dag(dag_id='xcom_dag_manual')
def xcoms_dag_manual():
    
    @task.python
    def first_task():
        print("Extracting data.... This is the first task")
        fetched_data = {"data":[1,2,3,4,5]}
        return fetched_data
    
    @task.python
    def second_task(data:dict):

        fetched_data = data['data']
        transformed_data = [x*2 for x in fetched_data]
        return {"transformed_data":transformed_data}
    
    
    
    @task.python
    def third_task(data:dict):
        
        load_data = data["transformed_data"]
        return load_data
    
    
    #defining task dependency
    first = first_task()
    second = second_task(first)
    third = third_task(second)


#instantiating the dag
xcoms_dag_manual()