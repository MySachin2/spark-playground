package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Properties.isWin

object BroadcastJoins {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")
  val spark = SparkSession.builder()
    .appName("TelecomProblem")
    .master("local[*]")
    .getOrCreate()

  // Huge Dataset

  val table = spark.range(1, 100_000_000)
  val rows: RDD[Row] = spark.sparkContext.parallelize(List(
    Row(1, "gold"),
    Row(2, "silver"),
    Row(3, "bronze")
  ))

  val rowSchema = StructType(
    Array(
      StructField("id", IntegerType),
      StructField("medal", StringType),
    )
  )

  val lookupTable: DataFrame = spark.createDataFrame(rows, rowSchema)

  val joined = table.join(lookupTable, "id")

  def main(args: Array[String]): Unit = {
//    joined.explain()
//    joined.show()

    // Broadcast Joins

    val joinedSmart = table.join(broadcast(lookupTable), "id")
    joinedSmart.explain()
    joinedSmart.show()
    Thread.sleep(10000000)
  }
}
