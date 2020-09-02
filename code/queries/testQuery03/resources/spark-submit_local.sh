#!/usr/bin/env bash
export SPARK_MAJOR_VERSION=2
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
/usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --class com.ual.hive.queries.QueryTest03 \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 4g \
        --executor-memory 2g \
        --executor-cores 1 \
        --queue IT_DS \
        --files /usr/hdp/current/spark-client/conf/hive-site.xml,/etc/tez/conf/tez-site.xml \
        testQuery03-1.0-SNAPSHOT.jar  --database=<database> --table=<table
