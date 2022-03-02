#!/bin/sh
RAW_DIR=$1
TRUSTED_DIR=$2

spark-submit weather_job.py $RAW_DIR $TRUSTED_DIR
