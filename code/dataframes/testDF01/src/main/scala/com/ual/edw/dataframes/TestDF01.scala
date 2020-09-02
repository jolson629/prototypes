package com.ual.edw.dataframes

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TestDF01 {

  private val logger = Logger.getLogger(getClass)

    // Simple join on a data frame

  def main(args: Array[String]): Unit = {
    logger.info("Starting Run....")

    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._


    val df = spark.read.json("resources/people.json")
    val df2 = df.join(df, df.col(df.columns(0)) === df.col(df.columns(0)))

    //val t = df2.col(df2.columns(0)) === df2.col(df2.columns(2)) && df2.col(df2.columns(1)) === df2.col(df2.columns(3))
    //val filteredCount = df2.filter(t).count()
    //df2.show()
    //logger.info("Matched: " +filteredCount)


    //val filteredCount = joinedDF.filter(($"source_city_cd" === $"target_city_cd") && ($"source_arpt_cd" === $"target_arpt_cd")).count()

  }
}
