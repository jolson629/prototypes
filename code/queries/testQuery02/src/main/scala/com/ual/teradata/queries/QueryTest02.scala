package com.ual.teradata.queries

import java.util.Properties

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession


object QueryTest02 {

  private val logger = Logger.getLogger(getClass)


  // https://spark.apache.org/docs/latest/sql-programming-guide.html
  // https://community.hortonworks.com/questions/63826/hi-is-there-any-connector-for-teradata-to-sparkwe.html


  // Test #2 - Connect to teradata using Scala and Spark 2.2, using a local spark instance and not in parallel to
  // to put a very small dataset into a spark data frame.
  def main(args: Array[String]): Unit = {

    val config = new TeradataArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    Class.forName("com.teradata.jdbc.TeraDriver")

    val teradata = SparkSession
      .builder()
      .appName("TestQuery02")
      .master(s"${config.sparkMaster}")
      .getOrCreate()

    import teradata.implicits._
/*
    val jdbcDF = teradata.read
      .format("jdbc")
      .option("url", s"jdbc:teradata://${config.teradataHost}/${config.teradataJdbcOptions}")
      .option("dbtable", s"${config.database}.${config.table}")
      .option("user", s"${config.user}")
      .option("password", s"${config.password}")
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .load()
*/

    val connectionProperties = new Properties
    connectionProperties.put("user", config.user)
    connectionProperties.put("password", config.password)

    val query = s"(select ${config.groupBy}, count(*) as cnt from ${config.database}.${config.table} group by ${config.groupBy}) as total order by ${config.groupBy}"

    val jdbcDF2 = teradata.read
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .jdbc(s"jdbc:teradata://${config.teradataHost}/${config.teradataJdbcOptions}",
        query, config.groupBy, 0, 0,1,connectionProperties)

    logger.info("Row count: "+jdbcDF2.count())

  }

}