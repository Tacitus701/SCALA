name := "Peaceland"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.4.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "2.1.0"

//libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.3" % Runtime
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.2",
  "org.apache.spark" %% "spark-sql" % "2.4.2"
)
libraryDependencies += "com.github.pjfanning" % "scala-faker_2.12" % "0.5.2"

libraryDependencies += "org.apache.hadoop" % "hadoop-azure-datalake" % "3.1.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-azure" % "3.1.1"

libraryDependencies += "com.google.code.gson" % "gson" % "2.7"