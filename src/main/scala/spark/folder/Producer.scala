import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

val props: Properties = new Properties()
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092")
props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

val key = "akey"
val value = "adata"

val record = new ProducerRecord[String, String]("atopic", key, value)
producer.send(record)

producer.close()