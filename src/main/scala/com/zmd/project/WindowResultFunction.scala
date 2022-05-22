package com.zmd.project

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang

class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  // Long  累加器的返回值
  // TimeWindow 第一步中的窗口类型 TimeWindow
  //
  override def apply(key: Long, w: TimeWindow, iterable: Iterable[Long], collector: Collector[ItemViewCount]): Unit = {

    val itemId: Long = key
    val count: Long = iterable.iterator.next()
    collector.collect(ItemViewCount(itemId, w.getEnd, count))
  }
}
