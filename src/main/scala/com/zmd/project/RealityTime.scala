package com.zmd.project

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object RealityTime {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val readStream: DataStream[String] = env.readTextFile("/Users/zmd/IdeaProjects/flinkTest/src/main/resources/UserBehavior.csv")
    val outputStream: DataStream[UserBehavior] = readStream.map(line => {
      val fileds: Array[String] = line.split(",")
      UserBehavior(fileds(0).toLong, fileds(1).toLong, fileds(2).toInt, fileds(3), fileds(4).toLong)
    }).assignAscendingTimestamps((_: UserBehavior).timestamp * 1000L) //提取watermark 定义watermark的生成

    val value: WindowedStream[UserBehavior, Long, TimeWindow] = outputStream
      .filter(_.behavior == "pv") //过滤出PV数据
      .keyBy(_.itemId) //按照商品ID进行分组
      .timeWindow(Time.minutes(60), Time.minutes(5))

    value //开窗进行统计
      .aggregate(new CountAgg(), new WindowResultFunction()) //聚合出当前商品在时间窗口内的统计数量
      .keyBy(1)
      .process(new TopNHotItems(3))
      .print()

    env.execute("output stream")
  }
}
