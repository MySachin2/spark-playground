package practice

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.github.javafaker.Faker
import grapple.json.{Json, JsonOutput}


object KafkaPersonProducer {

  val kafkaProps = new Properties()
  kafkaProps.put("bootstrap.servers", "localhost:9092")
  kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  // Kafka producer
  val producer = new KafkaProducer[String, String](kafkaProps)

  // Kafka topic
  val kafkaTopic = "sparkstream"

  val faker = new Faker()

  def generateRandomPerson(): Person = {
    val randomName = faker.name().name()
    val randomAge = faker.number().numberBetween(1, 100)
    Person(name = randomName, age = randomAge)
  }

  // Function to send a batch of Person records
  def sendBatch(): Unit = {
    for (_ <- 1 to 10) {
      val person = generateRandomPerson()
      val jsonString = Json.toJson(person).toString
      val record = new ProducerRecord[String, String](kafkaTopic, jsonString)
      producer.send(record)
    }
  }

  def main(args: Array[String]): Unit = {
    // Continuously send batches every 3 seconds
    try {
      while (true) {
        println("Sending Batches of Person")
        sendBatch()
        Thread.sleep(2000) // Sleep for 3 seconds
      }
    } finally {
      // Close the Kafka producer on program termination
      producer.close()
    }
  }
  
}