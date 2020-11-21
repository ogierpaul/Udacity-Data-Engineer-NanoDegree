docker run \
  -d \
  --name p5af \
  -p 8080:8080 \
  -v ./airflowcode/dags:/usr/local/airflow/dags \
  -v ./airflowcode/plugins:/usr/local/airflow/plugins \
  --rm \
  puckel/docker-airflow webserver
