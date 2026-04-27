package spout

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}

class RandomNumberSpout extends BaseRichSpout {

  var collector: SpoutOutputCollector = _

  override def open(conf: java.util.Map[String,AnyRef],
                    context: TopologyContext,
                    collector: SpoutOutputCollector): Unit = {
    this.collector = collector
  }

  override def nextTuple(): Unit = {
    val number = scala.util.Random.nextInt(100)

    println(s"🔥 Emitting: $number")

    collector.emit(new Values(number))
    Thread.sleep(1000)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("number"))
  }
}