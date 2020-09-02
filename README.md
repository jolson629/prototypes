# teraData
This project houses experimental Teradata - Spark code.

## Background
To run any code in this repository, you will need to download and install the [proprietary Teradata JDBC driver](https://downloads.teradata.com/download/connectivity/jdbc-driver).
You will need to set up an account with Teradata, and agree to their licensing agreement.

Once downloaded, unzip and install in your local maven repository:

```
mvn install:install-file -DgroupId=com.teradata.jdbc -DartifactId=tdgssconfig -Dversion=<version number, such as 16.00.00.32> -Dpackaging=jar -Dfile=<path to tdgssconfig.jar>

mvn install:install-file -DgroupId=com.teradata.jdbc -DartifactId=terajdbc4 -Dversion=<version number, such as 16.00.00.32> -Dpackaging=jar -Dfile=<path to terajdbc4.jar>

```

Also, all of these projects use a config file located in ./queries/(sub-project)/resources. Copy (sub-project name)_local.properties to (sub-project name).properties and fill in the appropriate values.

For many of the projects, in that same folder is a sample spark-submit script that you can use as a template to run the project on a Yarn based Spark cluster.

## Info
This project consists of the following sub-projects:

### Queries

#### testQuery01
This is a Java project that simply connects to Teradata using the Teradata JDBC driver, and performs a count on the given table.

#### testQuery02
This is a Scala project that creates a Spark session with Teradata using the Teradata JDBC driver, and performs a count on the given table. You can use "local[2]" as the spark-master
variable in the (sub-project name).properties file to run a standalone spark instance on the local machine.

#### testQuery03
This is a Scala project that creates a Spark session with Hive using a Spark 2.x session, and returns the contents of a Hive table. You can use "local[*]" as the spark-master
variable in the (sub-project name).properties file to run a standalone spark instance on the local machine.
Please note: you need to put the following files in ./src/main/resources if you want to run in the IDE: 
```
core-site.xml
hdfs-site.xml
hive-site.xml
```
These can be found on the cluster you want to connect with.

#### testQuery04
This is a Scala project that connects Spark to Teradata and executes a pure pushdown query. Useful for understanding how to read data at scale from Teradata into Spark.

#### testQuery05
This is a Scala project that connects Spark to Hive and executes a pure pushdown query. Useful for understanding how to read data at scale from Hive into Spark.



### Joins

#### testJoin01
This is a Scala project that creates a Spark session with both a connection to Teradata using the Teradata JDBC driver, and a connection to Hive. It then pulls a table from each into seperate dataframes, and compares the dataframes on a primary key. You can use "local[*]" as the spark-master.
variable in the (sub-project name).properties file to run a standalone spark instance on the local machine. This has been tested at very small scale (~50,000 rows).

If you want to run this project on a Yarn cluster, there is a sample spark-submit script in the project level resource folder.

There are two problems with this project currently:
1. It only runs in standalone mode, the build & submit to cluster scripts don't seem to be configured properly - the job will not deploy on a Spark / Tez cluster correctly.
2. The logic to compare the two dataframes is hardwired to four columns. There is a way to make this work on a dynamic number of columns in Scala.

### Dataframes

#### testDF01
This is a Scala project that is meant for experimenting with Spark dataframes.


## Running
Any project can be run from the root project directory. Open a command line prompt, and cd to the project root directory:

```
./gradlew :code:<project>:<sub-project name>:run
```
e.g.

```
./gradlew :code:queries:testQuery01:run
```


