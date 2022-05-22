package com.zmd.api

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.Random

class MyFlinkSource extends SourceFunction[sendor]{
  var running = true
  override def run(sourceContext: SourceFunction.SourceContext[sendor]): Unit = {
    val rand = new Random()

    //(id temp)
    var currentTemp = 1.to(1).map(i => ("sensor_" + 1, rand.nextDouble() * 100))

    //定义无限循环
    while (running) {
      //在=上次基础上更新
      currentTemp = currentTemp.map(
        data => {
          (data._1, data._2 + rand.nextGaussian()) //正态分布
        }
      )
      val current = System.currentTimeMillis()
      currentTemp.foreach(data => sourceContext.collect(sendor(data._1, current, data._2)))

      Thread.sleep(1000)
    }
  }


  override def cancel(): Unit = running = false


}
