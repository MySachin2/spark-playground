package practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.util.Properties.isWin
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkJoins {
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")

  val spark = SparkSession.builder().appName("sparkJoins").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  val kids = sc.parallelize(List(
    Row(40, "Mary", 1),
    Row(41, "Jane", 3),
    Row(42, "David", 3),
    Row(43, "Angela", 2),
    Row(44, "Charlie", 1),
    Row(45, "Jimmy", 2),
    Row(46, "Lonely", 7)
  ))

  val kidsSchema = StructType(Array(
    StructField("Id", IntegerType),
    StructField("Name", StringType),
    StructField("Team", IntegerType),
  ))

  val kidsDF = spark.createDataFrame(kids, kidsSchema)

  val teams = sc.parallelize(List(
    Row(1, "The Invincibles"),
    Row(2, "Dog Lovers"),
    Row(3, "Rockstars"),
    Row(4, "The Non-Existent Team")
  ))

  val teamsSchema = StructType(Array(
    StructField("TeamId", IntegerType),
    StructField("TeamName", StringType)
  ))

  val teamsDF = spark.createDataFrame(teams, teamsSchema)

  // Inner Join
  val joinCondition = kidsDF.col("Team") === teamsDF.col("TeamId")
  val kidsTeamDf = kidsDF.join(teamsDF, joinCondition)
  val allKidsTeamDf = kidsDF.join(teamsDF, joinCondition, "left_outer")
  val allTeamsDf = kidsDF.join(teamsDF, joinCondition, "right_outer")
  val allKidsTeamsFullDf = kidsDF.join(teamsDF, joinCondition, "full_outer")

  // Semi Joins
  val allKidsTeamsSemiDf = kidsDF.join(teamsDF, joinCondition, "left_semi")

  // Anti Join
  val allKidsTeamsAntiDf = kidsDF.join(teamsDF, joinCondition, "left_anti")

  // Cross Join
  val productKidsWithTeams = kidsDF.crossJoin(teamsDF)

  def main(args: Array[String]): Unit = {
    println("INNER JOIN")
    kidsTeamDf.show()
    println("LEFT OUTER JOIN")
    allKidsTeamDf.show()
    println("RIGHT OUTER JOIN")
    allTeamsDf.show()
    println("FULL OUTER JOIN")
    allKidsTeamsFullDf.show()
    println("SEMI JOIN")
    allKidsTeamsSemiDf.show()
    println("ANTI JOIN")
    allKidsTeamsAntiDf.show()
    println("CROSS JOIN")
    productKidsWithTeams.show()
  }
}
