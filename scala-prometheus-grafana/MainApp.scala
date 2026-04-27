import org.apache.storm.{Config, LocalCluster}
import org.apache.storm.topology.TopologyBuilder

import spout.RandomNumberSpout
import bolt.ProcessingBolt

import io.prometheus.client.exporter.HTTPServer

object MainApp extends App {

  val server = new HTTPServer(9095)
  println("Prometheus metrics at http://localhost:9095/metrics")

  val builder = new TopologyBuilder()

  builder.setSpout("spout", new RandomNumberSpout(), 1)

  builder.setBolt("bolt", new ProcessingBolt(), 1)
    .shuffleGrouping("spout")

  val config = new Config()
  config.setDebug(false)

  val cluster = new LocalCluster()
  cluster.submitTopology("storm-topology", config, builder.createTopology())

  println(" Storm topology started...")


  while (true) {
    Thread.sleep(1000)
  }
}