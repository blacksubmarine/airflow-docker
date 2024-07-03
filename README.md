image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.9.2}
image: ${AIRFLOW_IMAGE_NAME:-customising_airflow:latest}
image: ${AIRFLOW_IMAGE_NAME:-extending_airflow:latest}

FROM apache/airflow:2.9.2
COPY requirements.txt /requirements.txt 

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

