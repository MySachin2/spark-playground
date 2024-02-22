package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.*
import scala.util.Properties.isWin
import org.apache.spark.sql.*
import org.apache.spark.sql.types.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.expressions.Window

object TelecomProblem {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")
  val spark = SparkSession.builder()
    .appName("TelecomProblem")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits.{StringToColumn}


  def main(args: Array[String]): Unit = {
    val csvResourcePath = "src/main/resources/bank_account_link_data.csv"
    val bankAccountDf = spark.read.options(Map("header" -> "true", "inferSchema" -> "true")).csv(csvResourcePath)
    bankAccountDf
      .select($"MobileNumber".substr(0, 3).as("CC"))
      .filter($"LinkedtoBankAccount" === "Yes")
      .show()
  }
}
