package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.*
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

import scala.util.Properties.isWin


case class Company(
                    companyId: String,
                    companyName: String,
                    companyLocation: String,
                    companyAddress: String,
                    companyContact: Long,
                    profitMargin: Double,
                    establishedYear: Int
                  )



object CarProblem {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")
  val spark = SparkSession.builder()
    .appName("TelecomProblem")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext


  import spark.implicits._
  import scala3encoders.given



  def main(args: Array[String]): Unit = {

    val csvResourcePath = "src/main/resources/cars/CompanyInfo.csv"
    val companyInfoRdd: RDD[Company] = sc.textFile(csvResourcePath)
      .map(_.split(","))
      .map(row => Company(
        row(0),
        row(1),
        row(2),
        row(3),
        row(4).toLong,
        row(5).toDouble,
        row(6).toInt
      ))
    val orderResourcePath = "src/main/resources/cars/CompanyOrders.csv"

    val orderRdd = sc.textFile(orderResourcePath)
      .map(_.split(","))
      .map(row => Order(
        row(0),
        row(1),
        row(2),
        row(3),
        row(4),
        row(5).toLong,
        row(6).toDouble,
        row(7),
        row(8),
        row(9),
        row(10)
      ))

    val orderSchema = new StructType()
      .add(StructField("orderId", StringType, false))
      .add(StructField("companyId", StringType, false))
      .add(StructField("companyName", StringType, false))
      .add(StructField("companyLocation", StringType, false))
      .add(StructField("componentToBeManufactured", StringType, false))
      .add(StructField("quantity", LongType, false))
      .add(StructField("estimatedCost", DoubleType, false))
      .add(StructField("orderDate", StringType, false))
      .add(StructField("dueDate", StringType, false))
      .add(StructField("completionStatus", StringType, false))
      .add(StructField("deliveryStatus", StringType, false))


    val orderDf = orderRdd.toDF()

    val schema = new StructType()
      .add(StructField("companyId", StringType, false))
      .add(StructField("companyName", StringType, false))
      .add(StructField("companyLocation", StringType, false))
      .add(StructField("companyAddress", StringType, false))
      .add(StructField("companyContact", LongType, false))
      .add(StructField("profitMargin", DoubleType, false))
      .add(StructField("establishedYear", IntegerType, false))

    val companyInfoDf = companyInfoRdd.toDF()

    companyInfoDf
      .select($"companyName")
      .filter($"establishedYear".multiply(-1) + 2024 >= 20)

    companyInfoDf
      .join(orderDf.drop("companyName"), Seq("companyId"), "inner")
      .filter($"componentToBeManufactured" === "Shock Absorber")
      .groupBy($"companyName")
      .agg(
        sum($"estimatedCost").as("revenue")
      )
      .orderBy($"revenue".desc)
      .show()


    companyInfoRdd.map(company => (company.companyId, company.companyName))
      .join(orderRdd.filter(_.componentToBeManufactured == "Shock Absorber").map(order => (order.companyId, order.estimatedCost)))
      .map(_._2)
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(1)
      .foreach(println)

    val distinctCompanyCount = orderRdd
      .filter(_.componentToBeManufactured == "Fender")
      .map(order => order.companyId)
      .distinct()
      .count()

    println(distinctCompanyCount)

    val northCarolina = orderDf
      .filter($"companyLocation" === "North Carolina" && $"componentToBeManufactured" === "Exhaust Pipes")
      .count()
    println(northCarolina)

    val completedButNotDelivered = orderRdd
      .filter(_.completionStatus == "Completed")
      .filter(_.deliveryStatus != "Delivered")
      .count()
    println(completedButNotDelivered)

    val pendingOrders =
      orderDf
        .as[Order]
        .filter(_.completionStatus == "Pending")
        .map(order => (order.orderId, order.estimatedCost * 0.9))
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("discounted_orders")

      val totalCost =
        orderRdd
          .filter(_.completionStatus == "Pending")
          .filter(_.companyName == "Laser Wheels")
          .map((order) => (order.companyId, order.estimatedCost))
          .reduceByKey(_ + _)
          .first()

      println(totalCost)
  }
}

case class Order(
                  orderId: String,
                  companyId: String,
                  companyName: String,
                  companyLocation: String,
                  componentToBeManufactured: String,
                  quantity: Long,
                  estimatedCost: Double,
                  orderDate: String,
                  dueDate: String,
                  completionStatus: String,
                  deliveryStatus: String
                )
