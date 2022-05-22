package com.zmd.project

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.util.Random

class GenSourceMarketing extends RichParallelSourceFunction[MarketingUserBehavior] {

  var running = true
  val channelSet: Seq[String] = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviorTypes: Seq[String] = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")
  val rand: Random = Random

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements: Long = Long.MaxValue
    var count = 0L
    while (running && count < maxElements) {
      val id: String = UUID.randomUUID().toString
      val behaviorType: String = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel: String = channelSet(rand.nextInt(channelSet.size))
      val ts: Long = System.currentTimeMillis()
      sourceContext.collectWithTimestamp(MarketingUserBehavior(id, behaviorType, channel, ts), ts)
      count += 1
      TimeUnit.MILLISECONDS.sleep(500L)
    }
  }

  override def cancel(): Unit = running=false
}
