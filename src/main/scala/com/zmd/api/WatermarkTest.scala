package com.zmd.api

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object WatermarkTest {
  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.getConfig.setAutoWatermarkInterval(200)
    val windiwStream: DataStream[String] = environment.readTextFile("/Users/zmd/IdeaProjects/flinkTest/src/main/resources/data.txt")

    val dataStream = windiwStream.map(
      data => {
        val arr: Array[String] = data.split(", ")
        sendor(arr(0), arr(1).toLong, arr(2).toDouble)
      }
    )
      /*.keyBy("id")
      .timeWindow(Time.seconds(5))
      .allowedLateness(Time.seconds(1))*/

    val simpleStream: DataStream[sendor] = dataStream.assignAscendingTimestamps(_.timestamp * 1000L)

    val output: DataStream[sendor] = dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[sendor](Time.seconds(3)) {
      override def extractTimestamp(t: sendor) = {
        t.timestamp * 1000L
      }
    })
    //最大乱序如何设定？ 先设置小的数字 试水




  }
}
