package com.zmd.project

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date

class MarketingCountByChannel extends ProcessWindowFunction
  [((String,String),Long), MarketingViewCount, (String,String), TimeWindow]{
  override def process(
                        key: (String, String),
                        context: Context,
                        elements: Iterable[((String, String), Long)],
                        out: Collector[MarketingViewCount]): Unit = {
    val start: Long = context.window.getStart
    val end: Long = context.window.getEnd
    val channel: String = key._1
    val behaviorType: String = key._2
    val count: Int = elements.size

    out.collect(MarketingViewCount(format(start), format(end), channel, behaviorType, count))

  }

  private def format(ts: Long): String ={
    val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    df.format(new Date(ts))
  }
}
