# terraform init
terraform init
terraform plan
terraform apply
# terraform destroy # for destroy after apply

# ssh config
# check ip address
code ~/.ssh/config # ssh streamify-kafka

# set kafka
ssh streamify-kafka

git clone https://github.com/ndlongvn/streamify.git 

bash ~/streamify/scripts/vm_setup.sh && \
exec newgrp docker

export KAFKA_ADDRESS=IP.ADD.RE.SS

cd ~/streamify/kafka && \
docker-compose build && \
docker-compose up -d
    # go to port 9021 to see kafka ui

# set eventsim

bash ~/streamify/scripts/eventsim_startup.sh

docker logs --follow million_events

# set spark

ssh streamify-spark

git clone https://github.com/ndlongvn/streamify.git && \
cd streamify/spark_streaming

export KAFKA_ADDRESS=IP.ADD.RE.SS
export GCP_GCS_BUCKET=bigdata-project-it4931

# nano ~/streamify/google_credentials.json # and paster the content of the file in the nano editor

nohup spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 stream_all_events.py > spark_log.log &

tail -f spark_log.log

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar stream_to_gcs.py
# go to port 8088 or 9870 or 18080

# set airflow
ssh streamify-airflow

git clone https://github.com/ndlongvn/streamify.git && \
cd streamify

bash ~/streamify/scripts/vm_setup.sh && \
exec newgrp docker

# Move google_credentials.json file from local to the VM machine in ~/.google/credentials/ directory.  
# mkdir ~/.google
# mkdir ~/.google/credentials
cd ~/.google/credentials
nano google_credentials.json # and paster the content of the file in the nano editor



export GCP_PROJECT_ID=deft-manifest-406205
export GCP_GCS_BUCKET=bigdata-project-it4931

bash ~/streamify/scripts/airflow_startup.sh && cd ~/streamify/airflow

docker-compose logs --follow

# set dags

step1: load_songs_dag

step2: streamify_dag

step3: 

# destroy
terraform destroy



