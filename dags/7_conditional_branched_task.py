from airflow.sdk import dag,task


@dag(dag_id='conditional_branch_dag')
def conditional_branched_dags():
    
    @task.python
    def fetch_data_api(**kwargs):
        ti = kwargs['ti']
        print("Extracting data from API..")
        fetched_data ={"api_data":[1,2,3,4,5]}
        ti.xcom_push(key="return_result",value=fetched_data)
    
    @task.python
    def fetch_db_data(**kwargs):

        ti = kwargs["ti"]
        print("Extracting data from DB..")
        fetched_data = {"db_data":[15,12,19,18]}
        ti.xcom_push(key="return_result",value=fetched_data)
     
    
    @task.python
    def fetch_bucket_data(**kwargs):
        
        ti = kwargs["ti"]
        print("Extracting data from Bucket... ")
        fetched_data = {"file_data":[25,31,22,29,14]}
        ti.xcom_push(key="return_result",value=fetched_data)
    
    
    @task.python
    def merge_data(**kwargs):

        ti = kwargs["ti"]
        api_data =ti.xcom_pull(task_ids="fetch_data_api",key="return_result")["api_data"]
        db_data = ti.xcom_pull(task_ids="fetch_db_data",key="return_result")["db_data"]
        bucket_data = ti.xcom_pull(task_ids="fetch_bucket_data",key="return_result")["file_data"]

        loaded_data = api_data + db_data + bucket_data
        return loaded_data
    
    @task.python
    def skip_merge():

        return "Weekend, no merge needed"
    

    @task.branch
    def decider_task(**kwargs):

        from datetime import date


        # day = date.today().strftime("%a")
        day = date(2026,3,20).strftime("%a")

        if day in ["Sat","Sun"]:
            return "skip_merge"
        
        else:
            return "merge_data"



    
    
    #defining task dependency
    api_fetch = fetch_data_api()
    db_fetch = fetch_db_data()
    bucket_fetch = fetch_bucket_data()
    skip = skip_merge()
    merge = merge_data()

    [api_fetch, db_fetch, bucket_fetch ]>> decider_task() >> [merge,skip]



#instantiating the dag
conditional_branched_dags()