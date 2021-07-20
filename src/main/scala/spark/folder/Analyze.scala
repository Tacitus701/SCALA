package spark.folder

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}

import java.util.Properties
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.LocalDate
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import Producer.Message
import org.apache.spark.sql.Encoders
import faker._
import java.time.LocalDate

object Analyze {

  def main(args: Array[String]): Unit = {
		val session = PeaceSparkSession.sparkSession()
		
		import session.implicits._
    	val alerts = session.read.parquet("adl://peaceland.azuredatalakestore.net/Record")
		val n = alerts.count()
		//val encoder = Encoders.bean(classOf[Producer.Message])
		//val RDD = alerts.as[Producer.Message](encoder).rdd
		alerts.show()
		val RDD = alerts.map(row => Producer.Message(row.getInt(1), Geo.coords, row.getString(2), row.getDouble(3), row.getList(4),LocalDate.now())).rdd
		answer1(RDD, n)

		session.stop()
		//TODO apperler answer
  }

  def answer1(alerts: RDD[Message], count: Long) = {
		val r = alerts.map((e) => e.score).reduce(_ + _) / count
		println("The mean score is : ${r} .")
  }

	def answer2(alerts: RDD[Message]) = {
		val r = alerts.map(e => e.name).filter(e => e.startsWith("Dr.")).count()
		println("There are ${r} doctors in the alerts.")
  }
	
	def answer3(alerts: RDD[Message]) = {
		val r = alerts.map(e => e.pos._1).filter(e => e > 0).count()
		println("There are ${r} reports in the northern hemisphere.")
  }
	
	def answer4(alerts: RDD[Message]) = {
		val r = alerts.map(e => e.date).filter(e => e.isEqual(LocalDate.now())).count()
		println("There are ${r} reports today.")
  }
}