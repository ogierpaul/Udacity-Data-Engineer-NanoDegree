docker run \
  -d \
  --rm \
  --name pgcont \
  -p 5432:5432  \
  -v /Users/paulogier/79-Data/:/data \
  -e POSTGRES_PASSWORD=letmein \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_DB=mydb \
  postgres

docker exec -ti postgres /bin/bash

psql -U myuser -d mydb