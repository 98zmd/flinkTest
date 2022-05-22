package com.zmd.project.AdClickLog

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util.Date

class CountResultFormat extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {

    out.collect(CountByProvince(format(window.getEnd), key, input.iterator.next()))
  }
  private def format(ts: Long): String ={
    val df = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
    df.format(new Date(ts))
  }
}
