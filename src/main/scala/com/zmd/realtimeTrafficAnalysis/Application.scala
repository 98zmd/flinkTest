package com.zmd.realtimeTrafficAnalysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.text.SimpleDateFormat
import scala.util.matching.Regex

object Application {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val readStream: DataStream[String] = environment
      .readTextFile("/Users/zmd/IdeaProjects/flinkTest/src/main/resources/apache.log")

    readStream
      .map(data => {
      val arr: Array[String] = data.split(" ")
      ApacheLogEvent(arr(0),arr(2),changeTime(arr(3)),arr(5),arr(6))
    })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent] (Time.milliseconds(1000))
        { override def extractTimestamp(t: ApacheLogEvent): Long = { t.eventTime}
        })
      .filter((data: ApacheLogEvent) => {
        val pattern: Regex = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      } )
      .keyBy((_: ApacheLogEvent).url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(1)
      .process(new TopNHotUrls(5))
      .print()

    environment.execute("Network Flow Job")
  }

  def changeTime(date: String) ={
    new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(date).getTime
  }
}
