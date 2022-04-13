from airflow import DAG
from hyperdrive_spark_submit_operator.hyperdrive_spark_submit_operator import HyperdriveSparkSubmitOperator


#Place to merge template and user input properties

def build_hyperconfornamce_task(dag: DAG) -> DummyOperator:
    hyperconformance = HyperdriveSparkSubmitOperator(task_id="hyperconformance", name="hyperconformance", dag=dag)
    return hyperconformance

def build_offload_task(dag: DAG) -> DummyOperator:
    offload = HyperdriveSparkSubmitOperator(task_id="offload", name="offload", dag=dag)
    return offload


