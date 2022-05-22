package com.zmd.realtimeTrafficAnalysis

import com.zmd.project.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util
import scala.collection.mutable.ListBuffer

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Tuple,UrlViewCount,String]{

  private var listState: ListState[UrlViewCount] = _
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val urlListState = new ListStateDescriptor[UrlViewCount]("UrlListState", classOf[UrlViewCount])
    listState = getRuntimeContext.getListState(urlListState)
  }
  override def processElement(i: UrlViewCount,
                              context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    listState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allItems: ListBuffer[UrlViewCount] = ListBuffer()

    val items: util.Iterator[UrlViewCount] = listState.get().iterator()
    while(items.hasNext){
      allItems += items.next()
    }

    listState.clear()

    val sortedItems: ListBuffer[UrlViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    val result = new StringBuilder

    result.append("=================================\n")
    val stringTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp - 1)
    result.append("time: ").append(stringTime).append("\n")

    for(i <- sortedItems.indices){
      val currentItem: UrlViewCount = sortedItems(i)

      result.append("No").append(i+1).append(": ")
        .append("  url = ").append(currentItem.url)
        .append(" seeNums = ").append(currentItem.count).append("\n")
    }
    result.append("=================================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
