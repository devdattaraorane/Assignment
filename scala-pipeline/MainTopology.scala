package com.example.topology

import org.apache.storm.{Config, StormSubmitter}
import org.apache.storm.topology.TopologyBuilder
import org.apache.storm.kafka.spout.{KafkaSpout, KafkaSpoutConfig}

import com.example.bolts.{ParseBolt, TransformBolt, AerospikeBolt}

object MainTopology {

  def main(args: Array[String]): Unit = {

    val kafkaConfig = KafkaSpoutConfig
      .builder("localhost:9092", "user-events")
      .setGroupId("storm-group")
      .build()

    val kafkaSpout = new KafkaSpout[String, String](kafkaConfig)

    val builder = new TopologyBuilder()

    builder.setSpout("kafka-spout", kafkaSpout)

    builder.setBolt("parse-bolt", new ParseBolt(), 2)
      .shuffleGrouping("kafka-spout")

    builder.setBolt("transform-bolt", new TransformBolt(), 2)
      .shuffleGrouping("parse-bolt")

    builder.setBolt("aerospike-bolt", new AerospikeBolt(), 1)
      .shuffleGrouping("transform-bolt")

    val config = new Config()
    config.setDebug(true)
    config.setNumWorkers(1)

    StormSubmitter.submitTopology(
      "scala-pipeline",
      config,
      builder.createTopology()
    )
  }
}