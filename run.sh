#!/bin/bash -xe
# @author       Anthony Vilarim Caliani
# @contact      github.com/avcaliani

pip install -q -r requirements.txt

spark-submit --master local \
  --files resources/log4j.properties \
  --driver-java-options "-Dlog4j.configuration=file:resources/log4j.properties" \
  'src/main.py' "$@"
