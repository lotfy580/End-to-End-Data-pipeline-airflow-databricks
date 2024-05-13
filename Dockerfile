FROM apache/airflow:2.9.0-python3.9
USER root
RUN apt-get upgrade

USER airflow
COPY Dependencies.txt /Dependencies.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /Dependencies.txt