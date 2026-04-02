import org.apache.kafka.clients.producer._
import java.util.Properties

object ProducerApp extends App {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")   // WSL IP
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("enable.idempotence","true")
  props.put("acks", "all")  // ensure durability

  val producer = new KafkaProducer[String, String](props)

  val topic = "DevTopic"

  try {
    for (i <- 1 to 5) {
      val record = new ProducerRecord[String, String](topic, s"key$i", s"value$i")

      val metadata = producer.send(record).get()

      println(s"✅ Sent -> Partition: ${metadata.partition()}, Offset: ${metadata.offset()}")
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.flush()   //  ensure all messages are delivered
    producer.close()
  }
}