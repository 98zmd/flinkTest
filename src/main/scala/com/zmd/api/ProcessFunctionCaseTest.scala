package com.zmd.api

import org.apache.flink.streaming.api.scala._

object ProcessFunctionCaseTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val inputstream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    val keyedStream: KeyedStream[sendor, String] = inputstream.map(date => {
      val arr: Array[String] = date.split(",")
      sendor(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .keyBy(_.id)
    val dataStream: DataStream[String] = keyedStream.process(new KeyedProcessFunctionTest(3000))
    dataStream.print()

    environment.execute()
  }
}
