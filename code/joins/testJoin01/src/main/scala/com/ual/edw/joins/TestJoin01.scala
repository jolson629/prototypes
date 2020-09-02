package com.ual.edw.joins

import java.util.Properties
import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TestJoin01 {

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val config = new EDWArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run....")
    logger.info("   Source Query: "+config.sourceQuery)
    logger.info("   Source Join Field(s): "+config.sourceJoinField)
    logger.info("   Target Query: "+config.targetQuery)
    logger.info("   Target Join Field(s): "+config.targetJoinField)

    Class.forName("com.teradata.jdbc.TeraDriver")

    val sourceJoinFields = config.sourceJoinField.split(",")
    val targetJoinFields = config.targetJoinField.split(",")

    val teradata = SparkSession
      .builder()
      .appName("SparkSession")
      .master(s"${config.sparkMaster}")
      .enableHiveSupport()
      .getOrCreate()

    import teradata.implicits._
    import teradata.sql

    val connectionProperties = new Properties
    connectionProperties.put("user", config.teradataUser)
    connectionProperties.put("password", config.teradataPassword)
    connectionProperties.put("database", config.teradataDatabase)

    logger.info("Loading Source Records....")
    val sourceDF = teradata.read
      .option("driver", "com.teradata.jdbc.TeraDriver")
      .jdbc(s"jdbc:teradata://${config.teradataHost}/${config.teradataJdbcOptions}",
            config.sourceQuery,connectionProperties)

    if (config.sourceDisplayNumRecs > 0) {
      logger.info("Number of Source Records: "+sourceDF.count()+" Number of Columns in Source Table: "+sourceDF.columns.length+" Sample Source Records: ")

      // Include all key fields here, not just the first? Or lay the sort out explicitly in the config file?
      sourceDF.sort(sourceJoinFields(0)).show(config.sourceDisplayNumRecs, false)

      sourceDF.printSchema()
      val x = sourceDF.printSchema()
      logger.info("Source Schema: "+x)
    }

    logger.info("Loading Target Records....")
    val targetDF = teradata.sql(config.targetQuery)

    if (config.targetDisplayNumRecs >0) {
      logger.info("Number of Target Records: "+targetDF.count()+" Number of Columns in Target Table: "+sourceDF.columns.length +" Sample Target Records: ")
      // Include all key fields here
      targetDF.sort(targetJoinFields(0)).show(config.targetDisplayNumRecs, false)

      targetDF.printSchema()
    }

    for ( i <- 0 to (sourceJoinFields.length - 1)) {
      logger.info("Join field "+i+": "+sourceJoinFields(i)+" / "+targetJoinFields(i))
    }

    logger.info("Executing full outer join....")

    val joinColumns = for (i <- 0 to (sourceJoinFields.length - 1)) yield sourceDF.col(sourceJoinFields(i)) <=> targetDF.col(targetJoinFields(i))

    if (config.showExplain) {
      val joinedDF2 = sourceDF.join(targetDF, joinColumns.reduce((_&&_)), "fullouter").explain(true)
    }

    val joinedDF = sourceDF.join(targetDF, joinColumns.reduce((_&&_)), "fullouter")

    logger.info("Done with full outer join. Total number of joined records: "+joinedDF.count())
    if (config.fojDisplayNumRecs > 0) {
      logger.info("Sample full outer join:")

      joinedDF.sort(sourceJoinFields(0)).show(config.fojDisplayNumRecs, false)
      joinedDF.printSchema()
    }

    logger.info("Doing validation....")

    val validationColumns = for (i <- 0 to (sourceDF.columns.length-1)) yield (joinedDF.col(joinedDF.columns(i)) <=> joinedDF.col(joinedDF.columns(i+(sourceDF.columns.length))))

    val matchedDF = joinedDF.filter(validationColumns.reduce((_&&_)))
    var mismatchedCount: Long = 0

    logger.info("Done validation.")
    logger.info("Number of Matched records: "+matchedDF.count())
    if (config.matchDisplayNumRecs >0) {
      logger.info("Sample matched records: ")
      matchedDF.show(config.matchDisplayNumRecs, false)
    }
    var status = ""
    if (matchedDF.count() == joinedDF.count()) {
      status = "PASS! 100% Match!"
    }
    else {
      status = "FAIL!"

      logger.info("Generating mismatches...")

      // https://stackoverflow.com/questions/47167916/spark-scala-compare-two-columns-in-a-dataframe-when-one-is-null
      val mismatchColumns = for (i <- 0 to (sourceDF.columns.length-1)) yield (!(joinedDF.col(joinedDF.columns(i)) <=> joinedDF.col(joinedDF.columns(i+(sourceDF.columns.length)))))
      val mismatchedDF = joinedDF.filter(mismatchColumns.reduce((_||_)))

      mismatchedCount = mismatchedDF.count()
      logger.info("Done generating mismatches. Mismatched records: "+mismatchedCount)

      if (config.mismatchDisplayNumRecs > 0) {
        logger.info("Sample mismatched records: ")
        mismatchedDF.show(config.mismatchDisplayNumRecs, false)
      }
    }
    logger.info(status+" Total Records: "+ joinedDF.count()+" Matched: "+matchedDF.count()+" Mismatched: "+ mismatchedCount )
  }
}
