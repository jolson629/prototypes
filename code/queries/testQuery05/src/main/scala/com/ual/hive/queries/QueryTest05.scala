package com.ual.hive.queries

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object QueryTest05 {
  private val logger = Logger.getLogger(getClass)

  // Example #5 - Connect Spark to Hive - execute a pushdown query
  //
  // Please note: you need to put the following files in /src/main/resources if you
  // want to run in the IDE:
  //
  //  core-site.xml
  //  hdfs-site.xml
  //  hive-site.xml
  //
  // These can be found on the cluster you want to connect to

  def main(args: Array[String]): Unit = {

    val config = new HiveArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run....")

    val hiveSource = SparkSession
      .builder()
      .appName("TestQuery05")
      .master(s"${config.sparkMaster}")
      .enableHiveSupport()
      .getOrCreate()

    import hiveSource.implicits._
    import hiveSource.sql

    var df = hiveSource.sql(config.pushdownquery)
    df.show(500)
  }
}