package com.zmd.project

import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

class UserPageWindow extends AllWindowFunction[UserBehavior,UVCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UVCount]): Unit = {
    //val userids: mutable.Set.type = collection.mutable.Set
    var userId: Long = 0L
    var staticUserIDs: Set[Long] = Set[Long]()
    for(userid <- input){
      staticUserIDs += userid.userId
      userId = userid.userId
    }

    out.collect(UVCount(window.getEnd, userId, staticUserIDs.size))
  }
}
