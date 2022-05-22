package com.zmd.project

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.util
import scala.collection.mutable.ListBuffer

class TopNHotItems (topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{
  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])

    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }
  override def processElement(i: ItemViewCount,
                              context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    itemState.add(i)


    context.timerService().registerEventTimeTimer(i.windowEnd + 1)

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    val items: util.Iterator[ItemViewCount] = itemState.get().iterator()
    while(items.hasNext){
      allItems += items.next()
    }

    itemState.clear()

    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    val result = new StringBuilder

    result.append("=================================\n")
    val stringTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp - 1)
    result.append("time: ").append(stringTime).append("\n")

    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)

      result.append("No").append(i+1).append(": ")
        .append("  itemID = ").append(currentItem.itemId)
        .append(" seeNums = ").append(currentItem.count).append("\n")
    }
    result.append("=================================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}
