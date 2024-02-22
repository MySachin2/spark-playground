package practice
import scala.util.Properties.isWin
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkFileStreamer {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    if isWin then System.setProperty("hadoop.home.dir", System.getProperty("user.dir") + "\\winutils\\hadoop-3.3.1")
    val conf = new SparkConf().setAppName("HDFSStreamingApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val hdfsDirectoryPath = "hdfs://localhost:19000/user/sachi/data"
    val dataStream = ssc.textFileStream(hdfsDirectoryPath)
    ssc.checkpoint("hdfs://localhost:19000/user/sachi/checkpoint")
    val countDstream = dataStream.countByWindow(Seconds(5), Seconds(10))
    countDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
