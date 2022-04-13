from airflow.models import DAG
from datetime import datetime
from hyperdrive_templates.hyperdrive_templates import build_hyperconfornamce_task, build_offload_task


with DAG(
        dag_id='00_Hyperdrive-test-workflow',
        schedule_interval="@once",
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hyperdrive', "v1"]
) as dag:
    hyperconformance = build_hyperconfornamce_task(dag=dag)
    offload = build_offload_task(dag=dag)

    hyperconformance >> offload