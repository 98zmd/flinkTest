package com.zmd.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object UserView {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val readStream: DataStream[String] = environment
      .readTextFile("/Users/zmd/IdeaProjects/flinkTest/src/main/resources/UserBehavior.csv")

    readStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp*1000L)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.seconds(60*60))
      .apply(new UserPageWindow)
      .print()

    environment.execute()

  }
}
