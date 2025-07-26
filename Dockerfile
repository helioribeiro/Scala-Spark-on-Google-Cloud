# Multi-stage Dockerfile for Scala Spark Broadcast Join Project
FROM openjdk:11-jdk-slim as base

# Set environment variables
ENV SCALA_VERSION=2.12.20
ENV SPARK_VERSION=3.5.0
ENV SBT_VERSION=1.11.3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    vim \
    less \
    procps \
    bc \
    && rm -rf /var/lib/apt/lists/*

# Install Scala
RUN wget -q https://downloads.lightbend.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz \
    && tar -xzf scala-$SCALA_VERSION.tgz \
    && mv scala-$SCALA_VERSION /opt/scala \
    && rm scala-$SCALA_VERSION.tgz \
    && ln -s /opt/scala/bin/scala /usr/local/bin/scala \
    && ln -s /opt/scala/bin/scalac /usr/local/bin/scalac

# Install Apache Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz \
    && mv spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME \
    && rm spark-$SPARK_VERSION-bin-hadoop3.tgz

# Install SBT
RUN curl -L -o sbt-$SBT_VERSION.deb https://repo.scala-sbt.org/scalasbt/debian/sbt-$SBT_VERSION.deb \
    && dpkg -i sbt-$SBT_VERSION.deb \
    && rm sbt-$SBT_VERSION.deb

# Create app directory and set working directory
WORKDIR /app

# Copy project files
COPY build.sbt ./
COPY project/ ./project/
COPY src/ ./src/
COPY *.sh ./

# Make scripts executable
RUN chmod +x *.sh

# Create necessary directories
RUN mkdir -p data/parquet output /tmp/spark-events /tmp/sbt-server

# Build the project
RUN sbt clean compile test:compile assembly

# Create a non-root user
RUN useradd -m -s /bin/bash sparkuser && \
    chown -R sparkuser:sparkuser /app && \
    chown -R sparkuser:sparkuser /tmp/spark-events && \
    chown -R sparkuser:sparkuser /tmp/sbt-server

# Switch to non-root user
USER sparkuser

# Expose ports for Spark UI and History Server
EXPOSE 4040 18080

# Set default command
CMD ["/bin/bash"] 