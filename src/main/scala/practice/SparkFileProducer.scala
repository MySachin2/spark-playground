package practice

import grapple.json.Json
import org.apache.kafka.clients.producer.ProducerRecord
import practice.KafkaPersonProducer.{generateRandomPerson, producer}
import grapple.json.iterableToJsonArray
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.util.UUID
import scala.util.Properties.isWin

object SparkFileProducer {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val spark = SparkSession.builder().master("local[*]").appName("fileProducer").getOrCreate()
  val sc = spark.sparkContext

  def getPersonJson(n: Int): List[String] = {
    val personList = (1 to n).map { _ =>
      val person = generateRandomPerson()
      person
    }
    personList.map(Json.toJson(_).toString).toList
  }

  def getRandomFileName: String = {
    s"${UUID.randomUUID().toString}.json"
  }

  def main(args: Array[String]): Unit = {
    try {
      while (true) {
        println("Sending Batches of Person as JSON to HDFS")
        val person = getPersonJson(10)
        val hdfsOut = s"hdfs://localhost:19000/user/sachi/data/${getRandomFileName}"
        Thread.sleep(2000)
        val rdd = sc.parallelize(person)
        rdd.saveAsTextFile(hdfsOut)
      }
    } finally {
      // Close the Kafka producer on program termination
      producer.close()
    }
  }
}
