FROM apache/flink:1.17.2-scala_2.12-java11

USER root

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Make python3 the default python
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Install PyFlink and Kafka Python client
RUN pip3 install --no-cache-dir \
    apache-flink==1.17.2 \
    confluent-kafka==2.3.0

# Download Flink Kafka SQL connector JAR (for Table API)
RUN wget -q -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.17/flink-sql-connector-kafka-3.0.2-1.17.jar

# Copy the feature engineering job
COPY feature_engineering.py /opt/flink/usrlib/feature_engineering.py

# Set permissions
RUN chmod 644 /opt/flink/usrlib/feature_engineering.py && \
    chown -R flink:flink /opt/flink/usrlib/

USER flink
