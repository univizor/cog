#!/usr/bin/env bash
set -ex

JAR=./target/scala-2.11/cog-assembly-1.0.jar

sbt assembly && \
    spark-submit \
        --class cog.GeneralStats \
        --master $SPARK_MASTER \
        --num-executors 10 \
        --executor-memory 11g \
        --executor-cores 7  \
        $JAR \
        $SPARK_MASTER \
        /Users/otobrglez/Projects/univizor/u3/data/files/
