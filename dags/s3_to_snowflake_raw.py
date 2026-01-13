from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

@dag(
    dag_id="s3_to_snowflake_weather_raw",
    start_date=datetime(2025, 1, 1),
    schedule="5 10 * * *", 
    catchup=False
)
def s3_to_snowflake_weather_raw():

    SnowflakeOperator(
        task_id="copy_into_raw_weather_rio",
        snowflake_conn_id="snowflake_default",
        sql="""
        COPY INTO RAW.RAW_WEATHER_RIO
        FROM @olist_stage/weather/daily/rio_{{ ds }}.csv
        FILE_FORMAT = (TYPE = 'CSV', FIELD_DELIMITER = ',', SKIP_HEADER = 1);
        """
    )

s3_to_snowflake_weather_raw() 