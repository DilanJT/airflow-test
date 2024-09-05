FROM apache/airflow:2.7.1

# Install psycopg2-binary
RUN pip install psycopg2-binary==2.9.1
RUN pip install pytest
