import org.apache.kafka.clients.consumer._
import java.time.Duration
import java.util._
import scala.jdk.CollectionConverters._

object ConsumerApp extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")  // <-- SAME IP
  props.put("group.id", "demo-group-NEW")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("auto.offset.reset", "earliest")
  props.put("enable.auto.commit", "true")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(Collections.singletonList("DevTopic"))

  println("🚀 Consumer started...")

  while (true) {
    val records = consumer.poll(Duration.ofMillis(500))

    for (record <- records.asScala) {
      println(
        s"📩 Received -> Key: ${record.key()}, Value: ${record.value()}, Partition: ${record.partition()}, Offset: ${record.offset()}"
      )
    }
  }


}