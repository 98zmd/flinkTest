package com.zmd.project.LogFail

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object LogFail {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val readStream: DataStream[String] = environment
      .readTextFile("/Users/zmd/IdeaProjects/flinkTest/src/main/resources/AdClickLog.csv")

    readStream
      .map( data => {
        val arr: Array[String] = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(t: LoginEvent) = t.eventTime*1000L
      })
      .keyBy(_.userId)
      //.process(new )
      .print()
  }
}
