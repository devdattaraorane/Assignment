package bolt

import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.tuple.Tuple

import io.prometheus.client.{Counter, Gauge, Histogram}

class ProcessingBolt extends BaseRichBolt {

  var collector: OutputCollector = _

  @transient var processedCounter: Counter = _
  @transient var lastValueGauge: Gauge = _
  @transient var latency: Histogram = _
  @transient var errorCounter: Counter = _

  override def prepare(
                        conf: java.util.Map[String, AnyRef],
                        context: TopologyContext,
                        collector: OutputCollector
                      ): Unit = {

    this.collector = collector

    processedCounter = Counter.build()
      .name("storm_processed_total")
      .help("Total processed messages")
      .register()

    lastValueGauge = Gauge.build()
      .name("storm_last_value")
      .help("Last processed value")
      .register()

    latency = Histogram.build()
      .name("storm_processing_latency_seconds")
      .help("Processing latency")
      .register()

    errorCounter = Counter.build()
      .name("storm_errors_total")
      .help("Total errors")
      .register()
  }

  override def execute(input: Tuple): Unit = {

    val timer = latency.startTimer()

    try {
      val number = input.getIntegerByField("number")


      if (number % 10 == 0) {
        throw new RuntimeException("Random failure")
      }

      Thread.sleep(100)

      processedCounter.inc()
      lastValueGauge.set(number.toDouble)

      println(s"Processed: $number")

      collector.ack(input)

    } catch {
      case e: Exception => errorCounter.inc()

        println(s" Error: ${e.getMessage}")

        collector.fail(input)

    } finally {
      timer.observeDuration()
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
  }
}