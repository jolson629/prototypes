package com.ual.teradata.queries

import java.util.Properties

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object QueryTest04 {

  private val logger = Logger.getLogger(getClass)


  // Test #4 - Connect to teradata using Scala and Spark 2.2, and execute a pure pushdown query

  def main(args: Array[String]): Unit = {

    val config = new TeradataArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    Class.forName("com.teradata.jdbc.TeraDriver")

    val teradata = SparkSession
      .builder()
      .appName("TestQuery04")
      .master(s"${config.sparkMaster}")
      .getOrCreate()


    val connectionProperties = new Properties
    connectionProperties.put("user", config.user)
    connectionProperties.put("password", config.password)

    val jdbcDF2 = teradata.read
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .jdbc(s"jdbc:teradata://${config.teradataHost}/${config.teradataJdbcOptions}",
        config.pushdownquery,connectionProperties)

    logger.info("Row count: "+jdbcDF2.count())

  }

}
