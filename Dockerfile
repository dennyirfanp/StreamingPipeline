# Use Airflow 2.8.1 with Python 3.11
FROM apache/airflow:2.8.1-python3.11

USER root

# (Optional) Install OpenJDK 17 if needed for Spark/Hive
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (if needed)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install all required Python libraries
RUN pip install --no-cache-dir \
    matplotlib pandas sqlalchemy psycopg2-binary \
    "apache-airflow[statsd]==2.8.1" \
    "apache-airflow-providers-apache-spark<5" \
    "apache-airflow-providers-apache-hive<9"

USER airflow
