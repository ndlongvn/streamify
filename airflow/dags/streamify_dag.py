import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from schema import schema
from task_templates import (create_external_table, 
                            create_empty_table, 
                            insert_job, 
                            delete_external_table)

EVENTS = ['listen_events', 'page_view_events', 'auth_events'] # we have data coming in from three events


GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_GCS_BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'streamify_stg')

EXECUTION_MONTH = '{{ logical_date.strftime("%-m") }}'
EXECUTION_DAY = '{{ logical_date.strftime("%-d") }}'
EXECUTION_HOUR = '{{ logical_date.strftime("%-H") }}'
EXECUTION_DATETIME_STR = '{{ logical_date.strftime("%m%d%H") }}'
# EXECUTION_MINUTE = '{{ (((logical_date.minute//5)*5)) }}'
EXECUTION_MINUTE = '{{ "{:02d}".format(((logical_date.minute // 5) * 5) % 60) }}'
EXECUTION_FIVE_MINUTE_INTERVAL = '{{ (logical_date.strftime("%Y-%m-%d %H")) }}-{{ execution_minute }}'
# print(EXECUTION_FIVE_MINUTE_INTERVAL)

# EXECUTION_MINUTE = '{{ ((logical_date - timedelta(minutes=(logical_date.minute % 5))).strftime("%M")) }}'
# EXECUTION_FIVE_MINUTE_INTERVAL = '{{ ((logical_date - timedelta(minutes=(logical_date.minute % 5))).strftime("%Y%-m-%d %H")) }}-{{ execution_minute }}'

TABLE_MAP = { f"{event.upper()}_TABLE" : event for event in EVENTS}

MACRO_VARS = {"GCP_PROJECT_ID":GCP_PROJECT_ID, 
              "BIGQUERY_DATASET": BIGQUERY_DATASET, 
              "EXECUTION_DATETIME_STR": EXECUTION_DATETIME_STR,
            "EXECUTION_MINUTE": EXECUTION_MINUTE,
            "EXECUTION_FIVE_MINUTE_INTERVAL": EXECUTION_FIVE_MINUTE_INTERVAL
              }

MACRO_VARS.update(TABLE_MAP)

default_args = {
    'owner' : 'airflow'
}

with DAG(
    dag_id = f'streamify_dag',
    default_args = default_args,
    description = f'Hourly data pipeline to generate dims and facts for streamify',
    # schedule_interval="5 * * * *", #At the 5th minute of every hour
    schedule_interval="*/5 * * * *",
    start_date=datetime(2022,3,26,18),
    catchup=False,
    max_active_runs=1,
    user_defined_macros=MACRO_VARS,
    tags=['streamify']
) as dag:
    
    initate_dbt_task = BashOperator(
        task_id = 'dbt_initiate',
        bash_command = 'cd /dbt && dbt deps && dbt seed --select state_codes --profiles-dir . --target prod'
    )

    execute_dbt_task = BashOperator(
        task_id = 'dbt_streamify_run',
        bash_command = 'cd /dbt && dbt deps && dbt run --profiles-dir . --target prod'
    )

    for event in EVENTS:
        
        staging_table_name = event
        insert_query = f"{{% include 'sql/{event}.sql' %}}" #extra {} for f-strings escape
        external_table_name = f'{staging_table_name}_{EXECUTION_DATETIME_STR}'
        events_data_path = f'{staging_table_name}/month={EXECUTION_MONTH}/day={EXECUTION_DAY}/hour={EXECUTION_HOUR}'\
                            f'/five_minute_interval={EXECUTION_FIVE_MINUTE_INTERVAL}'
        events_schema = schema[event]

        create_external_table_task = create_external_table(event,
                                                           GCP_PROJECT_ID, 
                                                           BIGQUERY_DATASET, 
                                                           external_table_name, 
                                                           GCP_GCS_BUCKET, 
                                                           events_data_path)

        create_empty_table_task = create_empty_table(event,
                                                     GCP_PROJECT_ID,
                                                     BIGQUERY_DATASET,
                                                     staging_table_name,
                                                     events_schema)
                                                
        execute_insert_query_task = insert_job(event,
                                               insert_query,
                                               BIGQUERY_DATASET,
                                               GCP_PROJECT_ID)

        delete_external_table_task = delete_external_table(event,
                                                           GCP_PROJECT_ID, 
                                                           BIGQUERY_DATASET, 
                                                           external_table_name)
                    
        
        create_external_table_task >> \
        create_empty_table_task >> \
        execute_insert_query_task >> \
        delete_external_table_task >> \
        initate_dbt_task >> \
        execute_dbt_task