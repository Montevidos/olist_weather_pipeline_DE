from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
from src.daily_weather_rio import fetch_rio_weather_today, upload_rio_weather_to_s3
print(context["data_interval_start"].date())
@dag(
    dag_id = "weather_rio_dag",
    start_date = datetime(2025, 1, 1),
    schedule = "0 10 * * *",
    catchup = False
)
def rio_weather_daily():

    @task 
    def fetch():
        context = get_current_context()
        execution_date = context['ds']
        day = datetime.strptime(execution_date, "%Y-%m-%d").date()
        rio_data = fetch_rio_weather_today(day)
        return rio_data             
    @task
    def upload(rio_data):       
        context = get_current_context()
        execution_date = context['ds']
        day = datetime.strptime(execution_date, "%Y-%m-%d").date()
        upload_rio_weather_to_s3(rio_data, day)
    upload(fetch())
rio_weather_daily_dag = rio_weather_daily()