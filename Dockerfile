FROM apache/airflow:2.7.3
ADD requirements.txt .
RUN pip install apache-airflow==2.7.3 -r requirements.txt
