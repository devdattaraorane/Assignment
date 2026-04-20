package com.example.producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import org.json.JSONObject

import scala.util.Random

object JsonProducer extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val actions = Array("login", "purchase", "logout")

  while (true) {

    val json = new JSONObject()
    json.put("userId", Random.nextInt(100).toString)
    json.put("action", actions(Random.nextInt(actions.length)))
    json.put("amount", Random.nextInt(1000))
    json.put("timestamp", System.currentTimeMillis())

    val record = new ProducerRecord[String, String]("user-events", json.toString)

    producer.send(record)

    println(s"Sent: $json")

    Thread.sleep(1000)
  }
}