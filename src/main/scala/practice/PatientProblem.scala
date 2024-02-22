package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Properties.isWin

case class PatientRecord(
    regNo: String,
    patientName: String,
    age: String,
    dbo: String,
    gender: String,
    bloodG: String,
    department: String,
    doctor: String,
    insuranceId: String,
    backgroundStatus: String,
    dischargeApproval: String
)


case class InsuranceInfo(
  companyName: String,
  insuranceId: String,
  percentCover: Int,
  limit: Long
)

case class BillRecord(
  regNo: String,
  amountPaid: Long,
  totalAmount: Long
)

object PatientProblem {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val spark = SparkSession
    .builder()
    .appName("PatientProblem")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    import scala3encoders.given

    val billRdd: RDD[BillRecord] = sc
      .textFile("src/main/resources/patient/BillTillDate.txt")
      .map(_.split("\t"))
      .map(record => BillRecord(record(0), record(1).toLong, record(2).toLong))

    val patientRecordsRdd: RDD[PatientRecord] = sc.textFile("src/main/resources/patient/PatientRegistrationData.csv")
      .map(line => {
        val fields = line.split(",")
        PatientRecord(
          fields(0),
          fields(1),
          fields(2),
          fields(3),
          fields(4),
          fields(5),
          fields(6),
          fields(7),
          fields(8),
          fields(9),
          fields(10)
        )
      })

    val companyInfoRdd: RDD[InsuranceInfo] = sc.textFile("src/main/resources/patient/CompanyInsuranceData.csv")
      .map(line => {
        val fields = line.split(",")
        InsuranceInfo(
          fields(0),
          fields(1),
          fields(2).toInt,
          fields(3).toLong
        )
      })

    val patientToBeDischargedRDD: RDD[PatientRecord] =
      patientRecordsRdd
        .filter(_.backgroundStatus == "COMPLETED")
        .filter(_.dischargeApproval == "POSITIVE")

    val patientRegistrationDf = patientRecordsRdd.toDF
    val patientDischargeDf = patientToBeDischargedRDD.toDF
    val companyDf = companyInfoRdd.toDF
    val billDf = billRdd.toDF

    val commonInsuranceDf = patientDischargeDf
      .join(companyDf, Seq("insuranceId"), "inner")

    val approvedDf = commonInsuranceDf

    val billJoinedDf = approvedDf.join(billDf, "regNo")

    val remainingAmountDf = billJoinedDf.selectExpr("regNo", "(totalAmount - amountPaid) AS remainingAmount", "totalAmount", "percentCover")

    val coveredDf = remainingAmountDf.selectExpr("regNo", "(totalAmount * percentCover/100) AS coveredAmount")

    val coveredFullDf = remainingAmountDf.join(coveredDf, "regNo")

    val billAmountDf = coveredFullDf.selectExpr("regNo", "(totalAmount - coveredAmount) AS billAmount")

    val patientDf = approvedDf.join(billAmountDf, "regNo")
    patientDf.show()

    val patientRdd = patientDf.rdd
    val companyGroupedRdd = patientRdd
      .map(row => (row.getString(11), row.getDouble(14)))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    companyGroupedRdd.take(20).foreach(println)
    sc.stop()
  }


}
