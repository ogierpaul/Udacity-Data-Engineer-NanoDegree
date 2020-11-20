# Tell docker which base image to use
FROM jupyter/minimal-notebook

# use root access to install the necessaries config file for psycopg2
USER root
RUN apt-get update && apt-get install -y libpq-dev

# Switch to normal jupyter user
USER $NB_UID

# You can as well use a requirements.txt file (Path is relative to docker-compose.yml, See Step3)
RUN pip install pandas psycopg2 sqlalchemy && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# Launch the jupyter notebook
CMD ["jupyter", "notebook", "--allow-root"]
