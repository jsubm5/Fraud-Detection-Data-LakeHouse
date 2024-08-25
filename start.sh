#!/bin/bash
set -e 

# Start the first set of services: Spark, Nessie, MinIO, Zookeeper and Kafka
docker compose up -d \
  nessie \
  minio \
  mc \
  zookeeper \
  kafka

# write on env file to upate minio end point
# Making sure that the minio ip has been updated in spark container
minio_container_name='minio'
container_id=$(docker container ls -q --filter "name=${minio_container_name}")
if [ -z "$container_id" ]; then
  echo "container ${minio_container_name} is not found"
  exit 1
fi

container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $container_id)
echo "Minio IP: ${container_ip}"
sed -i "s|^MINIO_END_POINT.*|MINIO_END_POINT=http://${container_ip}:9000|" environment.env

docker compose up -d spark 
echo "Spark container 1"
# add secrets to login shell
# this allows executing S3 related spark-submit via SSH
# step 1: load environment variables
export $(grep -v '^\s*#.*' ./environment.env | xargs)
echo "Spark container 2"
# step 2: append 'export's in /etc/profile
docker exec -u root spark /bin/bash -c "\
echo 'export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY' >> /etc/profile && \
echo 'export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_KEY' >> /etc/profile && \
echo 'export AWS_REGION=$AWS_REGION' >> /etc/profile && \
echo 'export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION' >> /etc/profile"
echo "Spark container 3"
#init datalakehouse tables
docker exec spark spark-submit /includes/python_scripts/data_lakehouse_init.py
echo "Spark container 4"
# Run Spark Structured Streaming, Data Producer, and MongoDB loader
timeout 120s python ./includes/python_scripts/data-generators/start_kafka_producer.py > producer.log 2>&1 &
timeout 120s docker exec spark spark-submit /includes/ingestion/kafka/load-kafka-stream-transactions.py > spark_job.log 2>&1 &
wait

# shutting down kafka
# my poor computer can't handle all these containers, meh..
docker compose down zookeeper kafka
echo "**airflow"
# Initialize and start Airflow
docker compose -f airflow-compose.yaml up airflow-init
docker compose -f airflow-compose.yaml up -d
echo "** network connect airflow"
# Connect Spark master to the Airflow network
docker network connect airflow-network spark
echo "** network connect airflow-network spark"
# configure airflow ssh connector
# this has to be secret and created manually
# remember that this is not production setup
docker exec airflow-webserver /home/airflow/.local/bin/airflow \
    connections add 'my_ssh_connection' \
    --conn-type 'ssh' \
    --conn-login 'me' \
    --conn-password 'changeme' \
    --conn-host 'spark' \
    --conn-port '22'