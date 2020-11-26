FROM postgres:f14f876334e9
COPY ./Dockersetup/pgschemacreation/ /docker-entrypoint-initdb.d/
