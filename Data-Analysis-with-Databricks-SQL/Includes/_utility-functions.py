# Databricks notebook source
# A utility method common used in streaming demonstrations.
# Note: it is best to show the students this code the first time
# so that they understand what it is doing and why, but from 
# that point forward, just call it via the DA object.
def _block_until_stream_is_ready(query, min_batches=2):
    import time
    while len(query.recentProgress) < min_batches:
        time.sleep(5) # Give it a couple of seconds

    print(f"The stream has processed {len(query.recentProgress)} batchs")
    
DA.block_until_stream_is_ready = _block_until_stream_is_ready

# COMMAND ----------

# Used to initialize MLflow with the job ID when ran under test
def init_mlflow_as_job():
    import mlflow
    
    job_experiment_id = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(
        dbutils.entry_point.getDbutils().notebook().getContext().tags())["jobId"]

    if job_experiment_id:
        mlflow.set_experiment(f"/Curriculum/Experiments/{job_experiment_id}")

    init_mlflow_as_job()

    None # Suppress output

# COMMAND ----------

# This is a utility method used by N different lessons.
# Note we are not registering it against DA because we 
# don't need the students to know about it.
def create_magic_table():
    DA.paths.magic_tbl = f"{DA.paths.working_dir}"
    
    spark.sql("""
CREATE TABLE IF NOT EXISTS magic (some_number INT, some_string STRING)
USING DELTA
LOCATION '${da.paths.user_db}/magic'
""")
    

