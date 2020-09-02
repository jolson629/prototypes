package com.ual.kafka.streams


import com.beust.jcommander.JCommander
import org.apache.log4j.Logger
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source

object StreamTest02 {

  private val logger = Logger.getLogger(getClass)

  // Example #2 - Publish to a Kafka Topic
  //
  // Push messages to a Kafka topic


  def main(args: Array[String]): Unit = {
    val config = new StreamTestArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run....")

    val props = new Properties()
    props.put("bootstrap.servers", config.kafkaBrokers)
    props.put("client.id", "ScalaProducerExample")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val t = System.currentTimeMillis()
    val lines = Source.fromFile(config.inputFile).getLines.toArray

    val inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    for (line <- lines) {
      var adjustedLine = line
      if (config.includeEventTime) {
        adjustedLine = line.replace(line.substring(line.indexOf(" from ")),", '"+inputFormat.format(Calendar.getInstance().getTime())+"'"+line.substring(line.indexOf(" from ")))
      }
      logger.info("Writing line: "+adjustedLine)
      val data = new ProducerRecord[String, String](config.kafkaTopic, adjustedLine)
      producer.send(data)

    }

    logger.info("sent per second: " + lines.length * 1000 / (System.currentTimeMillis() - t))
    producer.close()

  }

}



