#!/usr/bin/env bash
export SPARK_MAJOR_VERSION=2
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
/usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --class com.ual.teradata.queries.QueryTest04 \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 16g \
        --executor-memory 16g \
        --executor-cores 10 \
        --queue IT_DS \
        --files  /etc/hive/conf/hive-site.xml \
        --jars terajdbc4.jar,tdgssconfig.jar,testQuery04-1.0-SNAPSHOT.jar \
        testQuery04-1.0-SNAPSHOT.jar --teradata-jdbc-options=LOGMECH=LDAP,CHARSET=UTF8 --teradata-host=<host> --user=<user> --password=<password> --pushdownquery='<query>'