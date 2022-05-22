package com.zmd.api

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SelfTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    environment.getConfig.setAutoWatermarkInterval(50)

    val inputstream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    val dataStream: DataStream[sendor] = inputstream.map(date => {
      val arr: Array[String] = date.split(",")
      sendor(arr(0), arr(1).toLong, arr(2).toDouble)
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[sendor](Time.seconds(3)) {
      override def extractTimestamp(t: sendor) = t.timestamp * 1000
    })
    val lateTag = new OutputTag[(String, Long, Double)]("late")

    val result: DataStream[(String, Long, Double)] = dataStream.map(data => (data.id, data.timestamp, data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateTag)
      .reduce((current, newData) => {
        (current._1, newData._2, current._3.min(newData._3))
      })
    result.getSideOutput(lateTag).print("late")
    result.print("result")

    environment.execute("job")
  }
}
