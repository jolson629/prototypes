#!/usr/bin/env bash
export SPARK_MAJOR_VERSION=2
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
/usr/hdp/<version>/spark2/bin/spark-submit --class com.ual.edw.joins.TestJoin01 \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 4g \
        --executor-memory 2g \
        --executor-cores 1 \
        --queue IT_DS \
        --files /usr/hdp/current/spark-client/conf/hive-site.xml,/etc/tez/conf/tez-site.xml \
        --jars terajdbc4.jar,tdgssconfig.jar,testJoin01-1.0-SNAPSHOT.jar \
        testJoin01-1.0-SNAPSHOT.jar
            --teradata-jdbc-options=LOGMECH=LDAP,CHARSET=UTF8
            --teradata-host=<host
            --teradata-database=<database
            --teradata-user=<user
            --teradata-password=<password
            --source-query='<query>'
            --target-query='<target>'
            --source-join-field=<join field is source query
            --target-join-field='<join field in target query>'
            --target-display-num-recs=100
            --source-display-num-recs=100
            --mismatch-display-num-recs=100
            --match-display-num-recs=100
            --foj-display-num-recs=100
            --show-explain=false
