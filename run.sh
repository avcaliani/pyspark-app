#!/bin/bash -xe
# @author       Anthony Vilarim Caliani
# @contact      github.com/avcaliani

pip install -q -r requirements.txt

spark-submit --master local \
  --files resources/log4j.properties \
  --packages "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0" \
  --driver-java-options "-Dlog4j.configuration=file:resources/log4j.properties" \
  'src/main.py' "$@"
