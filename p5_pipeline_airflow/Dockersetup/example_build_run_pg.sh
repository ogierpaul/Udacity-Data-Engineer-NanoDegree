docker build -t pgcont -f ./Dockersetup/postgres.dockerfile .

docker run \
  -d \
  --rm \
  --name pgcont \
  -p 5432:5432  \
  -v $PWD/data:/data \
  -e POSTGRES_PASSWORD=<password> \
  -e POSTGRES_USER=myuser \
  -e POSTGRES_DB=mydb \
  p5pg

docker exec -ti pgcont /bin/bash