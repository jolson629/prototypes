package com.ual.orc

import java.util.Properties

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object TestWriteHiveOrc01 {
  private val logger = Logger.getLogger(getClass)

  // Reads a Hive table containing XML. Extract keys, and write to a new or append to an
  // existing ORC file in the appropriate partition.

  //TODO: Switch logging over to JSON so we can elastic search it.

  def main(args: Array[String]): Unit = {

    val config = new WriteHiveOrcArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run...")
    val totalTime = System.currentTimeMillis()

    Class.forName("com.teradata.jdbc.TeraDriver")


    val teradata = SparkSession
      .builder()
      .appName("TestWriteHiveOrc01")
      .master(s"${config.sparkMaster}")
      .getOrCreate()


    val connectionProperties = new Properties
    connectionProperties.put("user", config.user)
    connectionProperties.put("password", config.password)

    val query = s"(" + config.sql +") as total " //order by "+config.orderBy"

    val jdbcDF2 = teradata.read
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .jdbc(s"jdbc:teradata://${config.teradataHost}/${config.teradataJdbcOptions}",
        query, connectionProperties)

    logger.info("Row count: "+jdbcDF2.count())
    logger.info("Rows: "+jdbcDF2.show())

    logger.info("Run Complete! Time: " + (System.currentTimeMillis() - totalTime) + " ms")

  }
}