package spark.folder

import faker._

import java.time.LocalDate
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object Producer {
  def main(args: Array[String]): Unit = {
    peacewatchers(100)
  }

  def peacewatchers(nbPeaceWatchers: Int): Unit = {
    val random = scala.util.Random

    val id = random.nextInt(nbPeaceWatchers)
    val pos = Geo.coords
    val name = Name.name
    val score = random.nextInt(100) / 10.0
    val words = Lorem.sentences()

    val msg = id + ";" + pos + ";" + name + ";" + score + ";" + words + ";" + LocalDate.now() + ";"
    
    if (score < 1)
      writetoKafka("alert", msg)

    writetoKafka("report", msg)

    Thread.sleep(random.nextInt(60000 / nbPeaceWatchers))
    peacewatchers(nbPeaceWatchers)
  }

  def writetoKafka(topic: String, msg: String): Unit = {

    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

    val record = new ProducerRecord[String, String](topic, msg)
    producer.send(record)

    producer.close()
  }
}