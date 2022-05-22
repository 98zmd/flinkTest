package com.zmd.project

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.addSource(new GenSourceMarketing()).print()

    val readStream: DataStream[MarketingUserBehavior] = environment.addSource(new GenSourceMarketing)

    readStream
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(1))
      .process(new MarketingCountByChannel)
      .print("result")
    environment.execute()
  }
}
