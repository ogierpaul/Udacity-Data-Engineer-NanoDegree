# Tell docker which base image to use
FROM jupyter/pyspark-notebook:42f4c82a07ff

USER root

# Install the missing jars
RUN wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar -P $SPARK_HOME/jars
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -P $SPARK_HOME/jars
# Expose port 4040 for Spark UI
EXPOSE 4040


# Install the additional libraries
RUN mkdir /home/jovyan/packages/
RUN mkdir /home/jovyan/packages/p4src/
ADD ./Dockersetup/p4src /home/jovyan/packages/p4src
ADD ./Dockersetup/setup.py /home/jovyan/packages/setup.py
ADD ./Dockersetup/etl.py /home/jovyan/etl.py
WORKDIR /home/jovyan/packages
RUN pip install -e .

# Add folder
RUN mkdir /home/jovyan/aws



# Switch to normal jupyter user
WORKDIR /home/jovyan
USER $NB_UID

# Launch the jupyter notebook
CMD ["jupyter", "notebook", "--allow-root"]
