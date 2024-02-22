package practice

import org.apache.hadoop.shaded.com.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import grapple.json.{ *, given }

object KafkaPersonStreamer {


  def main(args: Array[String]): Unit = {

    import scala3encoders.given

    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("SparkKafkaStreamTest")
    val sparkStreamingContext = new StreamingContext(sparkConfig, Seconds(10))
    sparkStreamingContext.sparkContext.setLogLevel("ERROR")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    import spark.implicits._

    val kafkaConfig = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "kafkaSparkTestGroup",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val kafkaTopics = Array("sparkstream")
    val kafkaRawStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        sparkStreamingContext,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](kafkaTopics, kafkaConfig)
      )

    val personStream: DStream[Person] =
      kafkaRawStream map {streamRawRecord =>
        val json = Json.parse(streamRawRecord.value())
        json.as[Person]
      }

    personStream.foreachRDD { rdd =>
      val df = rdd.toDF
      df.show(10, truncate = true)
      println(df.count())
    }
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
  }
}
