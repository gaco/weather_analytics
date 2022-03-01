# Pull base image
FROM amazoncorretto:8

RUN yum -y update
RUN yum -y install yum-utils
RUN yum -y groupinstall development

RUN yum list python3*
RUN yum -y install python3 python3-dev python3-pip python3-virtualenv

RUN python -V
RUN python3 -V

ENV PYSPARK_DRIVER_PYTHON python3
ENV PYSPARK_PYTHON python3

# Spark-Service Configurations
WORKDIR /spark_job

COPY ./spark_job/ .
COPY ./script_check.sh .

# Preparation
RUN pip3 install pyspark && chmod 755 script_check.sh

# App execution
ENTRYPOINT ["spark-submit", "/spark_job/weather_job.py", "file:///data/raw", "file:///data/trusted"]
