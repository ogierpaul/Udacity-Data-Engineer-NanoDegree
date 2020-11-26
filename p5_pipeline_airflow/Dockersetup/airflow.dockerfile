FROM puckel/docker-airflow:ce92b0f4d1d5
USER root
RUN pip install --upgrade boto3==1.16.0
CMD ["webserver"]
