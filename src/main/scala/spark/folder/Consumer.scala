package spark.folder

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}

import java.util.Properties
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import scala.collection.JavaConverters._

object Consumer {

  def main(args: Array[String]): Unit = {
    consumeFromKafka("test")
  }

  def consumeFromKafka(topic: String) = {

    val props: Properties = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumerGroup1")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(List(topic).asJava)

    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
      records.asScala.foreach({record =>
        println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
      })
    }
    //Si auto commit set to false
    //consumer.commitSync()
    //consumer.commitAsync()
  }

}