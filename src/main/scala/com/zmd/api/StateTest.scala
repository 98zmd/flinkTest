package com.zmd.api

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object StateTest {
  def main(args: Array[String]): Unit = {

    //key state using

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val inputstream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    val dataStream: DataStream[sendor] = inputstream.map(date => {
      val arr: Array[String] = date.split(",")
      sendor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 状态编程必须在富函数里面才能获取


    //lastTemp.value()

    environment.execute("state test")
  }
}
