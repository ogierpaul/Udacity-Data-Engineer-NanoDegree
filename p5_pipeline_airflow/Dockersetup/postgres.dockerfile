FROM postgres
COPY ./Dockersetup/pgschemacreation/ /docker-entrypoint-initdb.d/
