  docker run \
    -d \
    --name p4cont \
    -p 8888:8888 \
    -p 4040:4040 \
    -e JUPYTER_TOKEN=letmein \
    -v $PWD/notebooks:/home/jovyan/notebooks \
    -v $PWD/data:/home/jovyan/data \
    -v $PWD/aws/config.cfg:/home/jovyan/aws/config.cfg:ro \
    --user root \
    --rm \
    ogierpaul/p4spark