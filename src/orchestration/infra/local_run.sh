#!/bin/bash

docker build -t bliss/airflow:latest .

docker compose up bliss-airflow-init

docker compose down --volumes --remove-orphans

docker compose -f ./docker-compose.yaml -p bliss-airflow up -d
