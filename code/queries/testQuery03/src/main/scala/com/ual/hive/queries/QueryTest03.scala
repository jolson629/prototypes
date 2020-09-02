package com.ual.hive.queries

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object QueryTest03 {

  private val logger = Logger.getLogger(getClass)

  // Example #3 - Connect Spark to Hive
  // https://spark.apache.org/docs/latest/sql-programming-guide.html#hive-tables
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

    // Is there a better way to do this?

    val hiveSource = SparkSession
      .builder()
      .appName("TestQuery03")
      .master(s"${config.sparkMaster}")
      .enableHiveSupport()
      .getOrCreate()

    import hiveSource.implicits._
    import hiveSource.sql

    //val df = hiveSource.sql("SELECT * FROM "+s"${config.database}"+"."+s"${config.table}")
    val df = hiveSource.sql("select trim(departure_id) as target_departure_id, trim(cos_cd) as target_cos_cd, trim(bk_active_dtmz) as target_bk_active_dtmz, trim(rcrd_loc) as target_rcrd_loc, trim(creation_dt) as target_creation_dt, trim(action_cd) as target_action_cd FROM warehouse.booking_orc where flt_leg_dprt_dt = '2017-06-01' and value_path_orig_cd ='ORD'")
    df.show(5)
  }
}
