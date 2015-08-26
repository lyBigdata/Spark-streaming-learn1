package com.spark.apps

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}


/**
 * Created by liuyu on 8/23/15.
 */
object KafkaProducer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    // Zookeeper connection properties

    val producer = new Producer[String, String](new ProducerConfig(props))

    // Send some messages
    while(true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        val  message = new KeyedMessage[String, String](topic, str)
        producer.send(message)
      }

      Thread.sleep(1000)
    }
  }
}
