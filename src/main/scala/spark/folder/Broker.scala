package spark.folder

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes

import java.util.Properties
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
object Broker {
  def main(args: Array[String]): Unit = {
    streamKafka("test")
  }
  def streamKafka(topic: String) = {

    val streamsConfiguration = new Properties()
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "Broker1")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = new StreamsBuilder()

    val textlines = builder.stream[String, String](topic)

    val uppercasedWithMapValues = textlines.mapValues(_.toUpperCase())
    uppercasedWithMapValues.to("Uppercased_" + topic)

    val streams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()
  }
}
