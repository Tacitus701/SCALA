package spark.folder

import org.apache.spark.sql.SparkSession

object PeaceSparkSession {
  def sparkSession(): SparkSession = {
    val spark = SparkSession.builder().appName("peaceland").master("local").getOrCreate()

    spark.conf.set("fs.adl.oauth2.access.token.provider.type", "ClientCredential")
    spark.conf.set("fs.adl.oauth2.client.id", "88ab91f3-61f4-40a4-982a-35a622d3c31d")
    spark.conf.set("fs.adl.oauth2.credential", "S.KF40B2Jo8F~51e.~2_ySi1~byGSxJbHK")
    spark.conf.set("fs.adl.oauth2.refresh.url", "https://login.microsoftonline.com/3534b3d7-316c-4bc9-9ede-605c860f49d2/oauth2/token")
    spark
  }
}