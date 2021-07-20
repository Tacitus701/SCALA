package spark.folder

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._

object ConsumerReport {

  def main(args: Array[String])
  {
      consumeFromKafka("report", PeaceSparkSession.sparkSession())
  }

  def consumeFromKafka(topic: String, sparkSession: SparkSession) = {
    import sparkSession.implicits._

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
        sparkSession.read.json(Seq(record.value).toDS()).coalesce(1).write.mode(SaveMode.Overwrite).parquet("adl://peaceland.azuredatalakestore.net/Record")

        //println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}")
      })
    }
    //Si auto commit set to false
    //consumer.commitSync()
    //consumer.commitAsync()
  }

}