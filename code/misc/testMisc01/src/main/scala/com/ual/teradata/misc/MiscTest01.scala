package com.ual.teradata.misc

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger


object MiscTest01 {

  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val config = new TeradataArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Hello World!")
  }
}
