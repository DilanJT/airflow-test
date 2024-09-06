FROM apache/airflow:2.7.1

RUN pip install psycopg2-binary==2.9.1
RUN pip install pytest
