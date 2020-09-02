export SPARK_MAJOR_VERSION=2
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
/usr/hdp/2.6.1.0-129/spark2/bin/spark-submit --class com.ual.teradata.queries.QueryTest02 \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 4g \
        --executor-memory 2g \
        --executor-cores 1 \
        --queue IT_DS \
        --files  /etc/hive/conf/hive-site.xml \
        --jars terajdbc4.jar,tdgssconfig.jar,testQuery02-1.0-SNAPSHOT.jar \
        testQuery02-1.0-SNAPSHOT.jar --teradata-jdbc-options=LOGMECH=LDAP,CHARSET=UTF8 --teradata-host=<host> --user=<user> --password=<password> --database=<database> --table=<tablename> --spark-master=local[2] --group-by=<group by field>