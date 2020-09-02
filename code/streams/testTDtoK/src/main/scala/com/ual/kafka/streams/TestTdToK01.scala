package com.ual.kafka.streams

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import java.util.Properties
import org.apache.spark.sql.SparkSession

object TestTdToK01 {

  private val logger = Logger.getLogger(getClass)

  // Example #2 - XMLify a Teradata query and push to Kafka

  def main(args: Array[String]): Unit = {
    val config = new TDToKTestArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run....")

    Class.forName("com.teradata.jdbc.TeraDriver")

    val teradata = SparkSession
      .builder()
      .appName("TDToKTest01")
      .master(s"${config.sparkMaster}")
      .getOrCreate()

    val pushdownquery = "(select "+config.selectClause+" from "+config.teradataDatabase+"."+config.teradataTable+" "+config.whereClause+") as source"

    val connectionProperties = new Properties
    connectionProperties.put("user", config.user)
    connectionProperties.put("password", config.password)

    val jdbcDF2 = teradata.read
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .jdbc(s"jdbc:teradata://${config.teradataHost}/${config.teradataJdbcOptions}",
        pushdownquery,connectionProperties)

    logger.info("Row count: "+jdbcDF2.count())

    import teradata.implicits._
    import org.apache.spark.sql.functions._

    val startTime = System.currentTimeMillis()

    // add a serializer / deserializer in here at some point.
    // also, how to harden this?
    jdbcDF2
      // why oh why isn't there a to_xml?? Using to_json until I can write one.
      .select(to_json(struct($"*")).alias("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBrokers)
      .option("topic", config.kafkaTopic)
      .save()

    logger.info("Done! Wrote: "+jdbcDF2.count()+" records. Rate: "+jdbcDF2.count()*1000 / (System.currentTimeMillis() - startTime) +" msg/sec")
  }

}
