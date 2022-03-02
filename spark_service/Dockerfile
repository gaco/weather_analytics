# Pull base image
FROM amazoncorretto:11

RUN yum -y update && yum -y install yum-utils && yum -y groupinstall development && yum -y install initscripts

RUN yum -y install python3 python3-dev python3-pip

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

# Spark-Service Configurations
WORKDIR /spark_job

COPY ./spark_job/ .

# Preparation
RUN pip3 install pyspark && chmod -R 755 ./*.sh