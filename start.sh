#!/bin/bash
set -e 

# Start the first set of services: Spark, Nessie, MinIO, and Dremio
docker compose up -d \
  spark \
  nessie \
  minio \
  mc


# write on env file to upate minio end point
minio_container_name='minio'
container_id=$(docker container ls -q --filter "name=${minio_container_name}")
if [ -z "$container_id" ]; then
  echo "container ${minio_container_name} is not found"
  exit 1
fi

container_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $container_id)
echo "Minio IP: ${container_ip}"
sed -i "s|^MINIO_END_POINT.*|MINIO_END_POINT=http://${container_ip}:9000|" environment.env

#init datalakehouse tables
docker exec spark spark-submit /includes/python_scripts/data_lakehouse_init.py

# Start Zookeeper and Kafka in a separate command
docker compose up -d \
  zookeeper \
  kafka

sleep 10

# Run Spark Structured Streaming, Data Producer, and MongoDB loader
timeout 100s python ./includes/python_scripts/data-generators/start_kafka_producer.py &
timeout 100s docker exec spark spark-submit /includes/ingestion/kafka/load-kafka-stream-transactions.py &
wait

# Shut down Kafka and Zookeeper
docker compose down \
  zookeeper \
  kafka \
  mc

# # start dremio
docker compose up -d \
  dremio

# Initialize and start Airflow
docker compose -f airflow-compose.yaml up airflow-init
docker compose -f airflow-compose.yaml up -d

# Connect Spark master to the Airflow network
docker compose up -d \
  dremio