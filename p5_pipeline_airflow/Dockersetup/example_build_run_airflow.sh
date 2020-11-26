docker build -t ogierpaul/p5af -f ./Dockersetup/airflow.dockerfile .

docker run \
  -d \
  --name p5af \
  -p 8080:8080 \
  -v $PWD/airflowcode/dags:/usr/local/airflow/dags \
  -v $PWD/airflowcode/plugins:/usr/local/airflow/plugins \
  --rm \
  ogierpaul/p5af webserver

docker exec -ti p5af /bin/bash