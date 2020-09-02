package com.ual.orc

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object TestWriteOrc01 {

  private val logger = Logger.getLogger(getClass)

  // Reads a Hive table containing XML. Extract keys, and write to a new or append to an
  // existing ORC file in the appropriate partition.

  //TODO: Switch logging over to JSON so we can elastic search it.

  def main(args: Array[String]): Unit = {

    val config = new WriteOrcArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run...")
    val totalTime = System.currentTimeMillis()

    val hiveSource = SparkSession
      .builder()
      .appName("TestWriteOrc01")
      //.master(s"${config.sparkMaster}")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    import hiveSource.implicits._
    import org.apache.spark.sql.functions._

    val xmlHeader = config.xmlHeader
    val tag1 = config.tag1
    val tag2 = config.tag2

    val getDate = udf((s:String) => (scala.xml.XML.loadString(s) \ xmlHeader \ tag1).toString())
    val getNumber = udf((s:String) => (scala.xml.XML.loadString(s) \ xmlHeader \ tag2).toString())

    // TODO: Add the ability to express "last N minutes" so we can run from cron
    // For now, assume all in accordance with partitioning of source data.

    var timeClause = ""
    if (config.date != "") {
       timeClause = " where messagedate='"+config.date+"'"
    }

    if (config.startTime !="") {
      if (timeClause == "") {
        timeClause = "where "
      }
      else {
        timeClause = timeClause +" and "
      }
      timeClause = timeClause + "process_time_dtmz between '"+config.startTime+"' and '"+config.endTime+"'"
    }

    var limitClause = ""
    if (config.limit > 0) {
      limitClause = " limit "+config.limit
    }


    // Load data from Hive...
    val qry = "select " + config.select +" from " +config.database+"."+ config.table +timeClause + limitClause
    logger.info("Starting Hive read...")
    val startTimeRead = System.currentTimeMillis()
    val df = hiveSource.sql(qry)
    logger.info("Done read Hive. Time: " + (System.currentTimeMillis() - startTimeRead)+ " ms")

    // Transform data...
    logger.info("Transforming dataframe...")
    val startTimeXform = System.currentTimeMillis()
    val newDF = df.withColumn(config.columnName1, getDate($"raw_msg"))
                  .withColumn(config.columnName2, getNumber($"raw_msg"))
    logger.info("Done transforming dataframe. Time: " + (System.currentTimeMillis() - startTimeXform)+ " ms")

    // Do some logging...
    //TODO: Put these in the log properly
    newDF.show()
    newDF.printSchema()


    // Dump to HDFS partitioned
    // TODO: bucketing not supported, so cannot sort??
    logger.info("Starting message dump. Writing "+ newDF.count() +" messages to "+config.subdirs.split(",").size+" different subdirectories")
    val startTime = System.currentTimeMillis()

    // TODO: how do we validate all files got written correctly?

    config.subdirs.split(",").foreach{
      aSubdir => {
        logger.info("Writing "+ newDF.count() +" messages to "+config.rootDir+"/"+aSubdir)
        val startDumpTime = System.currentTimeMillis()
        newDF.write
          .partitionBy(config.columnName1, config.columnName2)
          //.sortBy("process_time_dtmz")
          .mode(SaveMode.Append)
          .format("orc")
          .option("compression","zlib")
          .save(config.rootDir+"/"+aSubdir)
        logger.info("Wrote "+ newDF.count() +" messages to "+config.rootDir+"/"+aSubdir+ " Time: "+(System.currentTimeMillis() - startDumpTime)+ "ms")
      }
    }
    logger.info("Done message dump. Wrote "+newDF.count()+" messages to "+config.subdirs.split(",").size+" different subdirectories. Time: " + (System.currentTimeMillis() - startTime)+ " ms")

    // TODO: This is not good. Find a better way to do this...
    logger.info("Generating partitions.")
    val startTimePartitions = System.currentTimeMillis()
    config.targetTables.split(",").foreach {
      tableName=> {
        val cmd1 = "MSCK REPAIR TABLE "+config.database+"."+tableName
        logger.info("Executing "+ cmd1)
        hiveSource.sql(cmd1)
      }
    }
    logger.info("Done partitioning all tables. Time: " + (System.currentTimeMillis() - startTimePartitions)+ " ms")

    // TODO: concatenate tables?? Do we need to do this? We are always appending.

    logger.info("Collecting Stats.")
    val startTimeCollectStats = System.currentTimeMillis()
    config.targetTables.split(",").foreach {
      tableName=> {
        val cmd1 = "analyze table "+config.database+"."+tableName+" partition("+config.columnName1+", "+ config.columnName2+") compute statistics"
        val cmd2 = "analyze table "+config.database+"."+tableName+" compute statistics for columns process_time_dtmz"
        logger.info("Executing "+ cmd1)
        hiveSource.sql(cmd1)
        logger.info("Executing "+ cmd2)
        hiveSource.sql(cmd2)
      }
    }
    logger.info("Done collecting stats. Time: " + (System.currentTimeMillis() - startTimePartitions)+ " ms")

    logger.info("Run Complete! Time: "+(System.currentTimeMillis() - totalTime)+ " ms")

  }
}
