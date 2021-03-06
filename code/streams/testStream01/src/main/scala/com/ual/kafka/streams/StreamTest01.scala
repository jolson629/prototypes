package com.ual.kafka.streams

import com.beust.jcommander.JCommander
import org.apache.log4j.Logger

import java.util.concurrent._
import java.util.{Collections, Properties}

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._




object StreamTest01 {
  private val logger = Logger.getLogger(getClass)

  class ScalaConsumerExample(val brokers: String,
                             val groupId: String,
                             val topic: String) extends Logging {

    val props = createConsumerConfig(brokers, groupId)
    val consumer = new KafkaConsumer[String, String](props)
    var executor: ExecutorService = null

    def shutdown() = {
      if (consumer != null)
        consumer.close();
      if (executor != null)
        executor.shutdown();
    }

    def createConsumerConfig(brokers: String, groupId: String): Properties = {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
      props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
      props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
      props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")
      props
    }

    def run() = {
      consumer.subscribe(Collections.singletonList(this.topic))

      Executors.newSingleThreadExecutor.execute(    new Runnable {
        override def run(): Unit = {
          while (true) {
            val records = consumer.poll(1000)

            for (record <- records) {
              logger.info("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
            }
          }
        }
      })
    }
  }


  // Example #1 - Consume a Kafka Topic
  //
  // Pull a text message from a Kafka topic


  def main(args: Array[String]): Unit = {
    val config = new StreamTestArgs
    new JCommander.Builder().addObject(config).build().parse(args.toArray: _*)

    logger.info("Starting Run....")

    val example = new ScalaConsumerExample(config.kafkaBrokers, config.kafkaGroupId, config.kafkaTopic)
    example.run()

  }

}
