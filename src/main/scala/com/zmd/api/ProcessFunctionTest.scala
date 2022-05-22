package com.zmd.api

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    /*environment.enableCheckpointing(1000L)//checkpoint执行间隔
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setCheckpointTimeout(60000L)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)//两次checkpoint最小的间隔时间 上面一行的配置失效
    environment.getCheckpointConfig.setPreferCheckpointForRecovery(true) //是否更倾向于用checkpoint做恢复  默认是false  于手动保存而言
    environment.getCheckpointConfig.setTolerableCheckpointFailureNumber(2) //容忍多少次checkpoint失败  checkpoint 挂掉的情况*/

    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,20000L)) //20秒重启5次

    environment.setRestartStrategy(RestartStrategies.failureRateRestart(1, Time.of(2, TimeUnit.SECONDS), Time.of(2, TimeUnit.SECONDS)))

    val inputstream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    val keyedStream: KeyedStream[sendor, String] = inputstream.map(date => {
      val arr: Array[String] = date.split(",")
      sendor(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .keyBy(_.id)
    val dataStream: DataStream[sendor] = keyedStream

    //keyedStream.process()
    environment.execute("ProcessFunctionTest")
  }
}
