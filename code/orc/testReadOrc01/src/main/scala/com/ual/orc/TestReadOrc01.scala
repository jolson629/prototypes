package com.ual.orc

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._



object TestReadOrc01 {

  private val logger = Logger.getLogger(getClass)


  // Reads an Orc file containing XML.

  def main(args: Array[String]): Unit = {

    val config = new ReadOrcArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run....")
    logger.info("   Orc Source: "+config.orcPath)

    val hiveSource = SparkSession
      .builder()
      .appName("TestReadOrc01")
      .master(s"${config.sparkMaster}")
      .enableHiveSupport()
      .getOrCreate()

    val testOrc = hiveSource.read.format("orc").load(config.orcPath)
    testOrc.createOrReplaceTempView("data")
    hiveSource.sql("Select * from data").collect.foreach(println)


  }


}
