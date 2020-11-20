FROM postgres
COPY ./Dockerconf/schemacreation/ /docker-entrypoint-initdb.d/
