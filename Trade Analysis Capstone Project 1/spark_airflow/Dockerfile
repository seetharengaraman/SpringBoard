FROM apache/airflow:2.4.2
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_HOME=/usr/local/lib/python3.8/site-packages/pyspark
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==2.1.3
COPY . $AIRFLOW_HOME
ADD dags $AIRFLOW_HOME/dags
WORKDIR $AIRFLOW_HOME
RUN pip install --no-cache-dir -r requirements.txt
COPY ./gcs-connector-latest-hadoop2.jar $SPARK_HOME/jars