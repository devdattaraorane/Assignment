package com.example.bolts

import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.tuple.{Tuple, Values, Fields}
import org.apache.storm.topology.OutputFieldsDeclarer

import java.util

class TransformBolt extends BaseRichBolt {

  private var collector: OutputCollector = _

  override def prepare(conf: util.Map[String, AnyRef],
                       context: TopologyContext,
                       collector: OutputCollector): Unit = {
    this.collector = collector
  }

  override def execute(input: Tuple): Unit = {

    val userId = input.getStringByField("userId")
    val amount = input.getIntegerByField("amount")

    val processedAmount = amount * 2

    collector.emit(input, new Values(userId, Int.box(processedAmount)))
    collector.ack(input)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("userId", "processedAmount"))
  }
}