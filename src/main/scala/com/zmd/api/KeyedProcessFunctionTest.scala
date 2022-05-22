package com.zmd.api

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class KeyedProcessFunctionTest(d: Long) extends KeyedProcessFunction[String, sendor, String]{

  //保存上一个温度值进行比较，保存注册时间，删除定时器用
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
  )
  lazy val timerTsStart: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("lastTemp", classOf[Long])
  )


  protected override def processElement(i: sendor, //输入的数据
                              context: KeyedProcessFunction[String, sendor, String]#Context, //上下文
                              collector: Collector[String]): Unit = { //输出数据

    //状态拿出来
    val lasttemp = lastTemp.value()
    val timeTs = timerTsStart.value()

    lastTemp.update(i.temperature)

    //判断当前温度和上次温度比较
    if(i.temperature > lasttemp && timeTs == 0){
      //注册定时器 如果温度上升，且没有定时器 注册定时器
      val ts = context.timerService().currentProcessingTime() + d
      context.timerService().registerProcessingTimeTimer(ts)
      timerTsStart.update(ts)
    }else if(i.temperature < lasttemp){
      context.timerService().deleteProcessingTimeTimer(timeTs)
      timerTsStart.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, sendor, String]#OnTimerContext, out: Collector[String]): Unit = {

    out.collect("传感器连续"+ctx.getCurrentKey + " " + d/1000 + "连续上升")
  }

}
