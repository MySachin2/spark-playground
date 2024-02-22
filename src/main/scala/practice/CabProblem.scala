package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, desc, max, month, to_date, to_timestamp, year}

import scala.util.Properties.isWin

extension(str: String) {
  def toCamelCase: String = {
    val words = str.split("_")
    words.head + words.tail.map(_.capitalize).mkString
  }
}

object CabProblem {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val spark = SparkSession.builder().appName("cabProblem").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    import scala3encoders.given

    val cabDf = spark.read.json("src/main/resources/cab/Cab_data.json")
    val passengerInitDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/cab/Passenger_data.csv")

    val transactionColumns = Seq("transactionId", "passengerId", "cabId", "typeBooked", "typeAlloted", "transactionDate", "timestamp", "source", "destination", "rating", "price", "couponCode", "travelTime", "waitTime")
    val transactionDf = spark.read.csv("src/main/resources/cab/Transaction_data.csv")
      .toDF(transactionColumns: _*)
      .withColumn("transactionDate", to_date(col("transactionDate"), "M/d/yyyy"))

    val currentColumns = passengerInitDf.columns
    val newColumns = currentColumns.map(_.toLowerCase.toCamelCase)
    val passengerDf = passengerInitDf.toDF(newColumns: _*)
    transactionDf
      .filter(
        year($"transactionDate") === 2020 && month($"transactionDate") === 3
      )
      .groupBy($"passengerId")
      .count()
      .orderBy(desc("count"))
      .show(1)

    transactionDf
      .groupBy($"passengerId", $"source")
      .count()
      .withColumnRenamed("count", "sourceCount")
      .orderBy(desc("sourceCount"))
      .show()

    transactionDf
      .groupBy($"passengerId", $"destination")
      .count()
      .withColumnRenamed("count", "sourceCount")
      .orderBy(desc("sourceCount"))
      .show()

    transactionDf
      .groupBy("source")
      .agg(
        avg($"waitTime").as("averageWaitTime")
      ).orderBy(desc("averageWaitTime"))
      .limit(5)
      .show()
  }
}
