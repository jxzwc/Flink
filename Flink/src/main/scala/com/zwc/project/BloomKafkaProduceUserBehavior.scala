package com.zwc.project

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
/*Uv统计的布隆过滤器 Kafka生产者生产数据*/
object BloomKafkaProduceUserBehavior {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092")
    props.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    props.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    val producer = new KafkaProducer[String, String](props)

    val bufferedSource = io.Source.fromFile("UserBehavior.csv绝对路径")
    for (line <- bufferedSource.getLines) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
