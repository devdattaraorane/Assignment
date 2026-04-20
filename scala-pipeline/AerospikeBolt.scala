package com.example.bolts

import com.aerospike.client.{AerospikeClient, Bin, Key}

import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.tuple.Tuple
import org.apache.storm.topology.OutputFieldsDeclarer

import java.util

class AerospikeBolt extends BaseRichBolt {

  private var client: AerospikeClient = _

  override def prepare(conf: util.Map[String, AnyRef],
                       context: TopologyContext,
                       collector: OutputCollector): Unit = {
    client = new AerospikeClient("localhost", 3000)
  }

  override def execute(input: Tuple): Unit = {

    val userId = input.getStringByField("userId")
    val amount = input.getIntegerByField("processedAmount")

    val key = new Key("test", "users", userId)
    val bin = new Bin("total_spent", amount)

    client.put(null, key, bin)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    // No output
  }
}