#!/usr/bin/env bash
set -e

IMAGE_NAME=univizor/spark-docker
SPARK_MASTER=spark://master:7077

docker run -ti \
  --rm \
  --link cog_master_1:master \
  -p 4040:4040 \
  -v `pwd`:/home/app \
  --net=cog_default \
  $IMAGE_NAME spark-submit \
  --master $SPARK_MASTER \
  --conf "spark.master=$SPARK_MASTER" $@
