package com.zmd.api

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowApi {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //val windiwStream: DataStream[String] = environment.readTextFile("/Users/zmd/IdeaProjects/flinkTest/src/main/resources/data.txt")

    val windiwStream: DataStream[sendor] = environment.addSource(new MyFlinkSource)
    val dataStream: DataStream[sendor] = windiwStream

    //dataStream
      //.map(sn => (sn.id, sn.temperature))
      //.keyBy(_._1)
      //.window( TumblingEventTimeWindows.of(Time.seconds(15)) )
      //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(10)))
      //.window(EventTimeSessionWindows.withGap(Time.seconds(10))) //消息超过十秒中 切换窗口
      //.timeWindow(Time.seconds(15), Time.seconds(10)) //简写版
      //.countWindow(5)
      //分组操作 计算还需要自己定义  窗口函数  --增量聚合函数  --全窗口函数
      /*
       * --增量聚合函数
       *    来一个处理一个 保持一个简单的状态
       * --全窗口函数
       *    收集数据 批处理  可获得窗口信息
       */


    val latetag = new OutputTag[(String, Long, Double)]("late")
    //统计窗口内的最小值
    val result: DataStream[(String, Long, Double)] = dataStream
      .map(sn => (sn.id, sn.timestamp, sn.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(latetag)
      //.minBy(1)
      .reduce(
        (current, newdata) => {
          println(newdata)
          (current._1, newdata._2, current._3.min(newdata._3))
        }
      )
    result.getSideOutput(latetag).print("late")
    result.print("result")

    environment.execute("test ")
  }
}
