FROM apache/airflow:2.9.2
USER root
# Installation de Java (requis pour Spark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow
# Installation de PySpark
RUN pip install pyspark==3.5.0