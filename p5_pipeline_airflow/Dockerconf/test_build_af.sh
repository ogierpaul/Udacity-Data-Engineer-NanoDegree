docker build -t wdaf -f airflow.dockerfile .
docker run \
  --name afc \
  -p 8080:8080 \
  -v /Users/paulogier/81-GithubPackages/Udacity-Data-Engineer-NanoDegree/withdocker/airflowdags:/usr/local/airflow/dags \
  wdaf webserver
