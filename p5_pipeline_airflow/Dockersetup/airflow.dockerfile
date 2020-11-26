FROM puckel/docker-airflow
USER root
RUN pip install --upgrade boto3==1.16.0
CMD ["webserver"]
