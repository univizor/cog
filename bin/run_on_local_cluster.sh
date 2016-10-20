#!/usr/bin/env bash
set -ex

SPARK_MASTER=spark://zbook.local:7077
JAR=./target/scala-2.11/cog-assembly-1.0.jar

sbt assembly && \
    ../spark-bin-hadoop/bin/spark-submit \
        --class cog.GeneralStats \
        --master $SPARK_MASTER \
        --num-executors 10 \
        --executor-memory 11g \
        --executor-cores 7  \
        $JAR \
        spark://zbook.local:7077 \
        /Users/otobrglez/Projects/univizor/u3/data/files/
