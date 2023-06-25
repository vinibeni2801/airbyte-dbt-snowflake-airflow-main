# https://docs.astronomer.io/learn/airflow-dbt

from pendulum import datetime

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.edgemodifier import Label

# https://airflow.apache.org/docs/apache-airflow-providers-dbt-cloud/stable/index.html
# https://cloud.getdbt.com/next/deploy/125424/projects/188618/jobs/167995
DBT_CLOUD_CONN_ID = "dbt"
JOB_ID = "167995"


def _check_job_not_running(job_id):
    """
    retrieves the last run for a given dbt cloud job a
    nd checks to see if the job is not currently running.
    """
    hook = DbtCloudHook(DBT_CLOUD_CONN_ID)
    runs = hook.list_job_runs(job_definition_id=job_id, order_by="-id")
    latest_run = runs[0].json()["data"][0]

    return DbtCloudJobRunStatus.is_terminal(latest_run["status"])


@dag(
    dag_id="mds-airbyte-dbt-transforms",
    start_date=datetime(2022, 11, 30),
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
    doc_md=__doc__,
    tags=['development', 'elt', 'astrosdk', 'transform', 'python', 'sql']
)
def check_before_running_dbt_cloud_job():
    begin, end = [EmptyOperator(task_id=id) for id in ["begin", "end"]]

    check_job = ShortCircuitOperator(
        task_id="check_job_is_not_running",
        python_callable=_check_job_not_running,
        op_kwargs={"job_id": JOB_ID},
    )

    trigger_job = DbtCloudRunJobOperator(
        task_id="trigger_dbt_cloud_job",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=JOB_ID,
        check_interval=600,
        timeout=3600,
    )

    begin >> check_job >> Label("job not currently running. proceeding.") >> trigger_job >> end


dag = check_before_running_dbt_cloud_job()