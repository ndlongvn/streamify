docker cp ../spark_streaming spark-master:/home/
docker exec -it spark-master /bin/bash

# install gg cloud sdk
curl https://sdk.cloud.google.com | bash 
exec -l $SHELL
gcloud init


spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 /home/spark_streaming/stream_all_events.py



## new command:
docker build -t spark-master -f Dockerfile_spark_master .