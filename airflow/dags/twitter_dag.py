from curses.ascii import SP
from datetime import datetime
from os.path import join
from airflow.models import DAG
from airflow.operators.alura import TwitterOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with DAG(dag_id="twitter_dag", start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_aluraonline",
        query="AluraOnline",
        file_path=join(
            "/Users/rbottega/Documents/alura/datapipeline/datalake",
            "twitter_aluraonline",
            "extract_date={{ ds }}",
            "AluraOnline_{{ ds_nodash }}.json"
        )
    )

    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_aluraonline",
        application=(
            "/run/datapipeline/spark/transformation.py"
        ),
        name = "twitter_transformation",
        application_args=[
            "--src",
            "/run/datapipeline/datalake/bronze/twitter_aluraonline/extract_date=2022-01-28",
            "--dest",
            "/run/datapipeline/datalake/silver/twitter_aluraonline",
            "--process-date",
            "{{ ds }}"
        ]
    )
