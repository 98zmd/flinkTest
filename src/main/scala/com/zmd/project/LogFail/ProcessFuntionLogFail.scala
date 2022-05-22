package com.zmd.project.LogFail

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class ProcessFuntionLogFail extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {

  lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("saved Login", classOf[LoginEvent])
  )
  
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, collector: Collector[LoginEvent]): Unit = ???
}
