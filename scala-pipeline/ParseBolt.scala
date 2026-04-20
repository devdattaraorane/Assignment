package com.example.bolts

import org.apache.storm.topology.base.BaseRichBolt
import org.apache.storm.task.{OutputCollector, TopologyContext}
import org.apache.storm.tuple.{Tuple, Values, Fields}
import org.apache.storm.topology.OutputFieldsDeclarer

import java.util
import org.json.JSONObject

class ParseBolt extends BaseRichBolt {

  private var collector: OutputCollector = _

  override def prepare(conf: util.Map[String, AnyRef],
                       context: TopologyContext,
                       collector: OutputCollector): Unit = {
    this.collector = collector
  }

  override def execute(input: Tuple): Unit = {

    val jsonStr = input.getStringByField("value")

    try {
      val json = new JSONObject(jsonStr)
      val action = json.getString("action")

      if (action == "purchase") {
        val userId = json.getString("userId")
        val amount = json.getInt("amount")

        collector.emit(input, new Values(userId, Int.box(amount)))
      }

      collector.ack(input)

    } catch {
      case _: Exception =>
        collector.fail(input)
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("userId", "amount"))
  }
}