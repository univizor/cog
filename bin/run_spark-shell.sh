#!/usr/bin/env bash
set -e

spark-shell --master $SPARK_MASTER \
	--num-executors=10 \
	--jars=./target/scala-2.11/cog-assembly-1.0.jar
