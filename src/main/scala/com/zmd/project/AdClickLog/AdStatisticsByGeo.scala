package com.zmd.project.AdClickLog

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object AdStatisticsByGeo {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.setParallelism(1)

    val readStream: DataStream[String] = environment
      .readTextFile("/Users/zmd/IdeaProjects/flinkTest/src/main/resources/AdClickLog.csv")

    readStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        AdClickLog(arr(0).toLong, arr(1).toLong, arr(2), arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.province)
      .timeWindow(Time.minutes(60), Time.seconds(5))
      .aggregate(new CountAgg, new CountResultFormat)
      .print()

    environment.execute()
  }
}
