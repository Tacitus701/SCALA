package spark.folder

import java.time.LocalDate

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object Producer {
  def main(args: Array[String]): Unit = {
    peacewatchers(100)
  }

  val people = List("Robin Vandamme", "Arnaud Charpentier", "Jean-Christophe Antoine", "Philippe Qiao")
  val peaceScores = List(10, 0, 0, 0)
  val words = List("bonjour", "hello", "revolution", "pain")


  def peacewatchers(nbPeaceWatchers: Int): Unit = {
    val random = scala.util.Random

    val id = random.nextInt(nbPeaceWatchers)
    val latitude = random.nextInt(180) - 90
    val longitude = random.nextInt(360) - 180
    val index = random.nextInt(people.length)
    val name = people(index)
    val score = peaceScores(index)

    val msg = id + ";" + latitude + ";" + longitude + ";" + name + ";" + score + ";" + words.mkString(",") + ";" + LocalDate.now() + ";"

    println(msg)

    if (score < 5)
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