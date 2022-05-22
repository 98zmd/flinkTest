package com.zmd.api

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.util.Collector

class TempChangeAlert(d: Double) extends RichFlatMapFunction[sendor ,(String, Double, Double)]{

  //定义状态保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(
    new ValueStateDescriptor[Double]("valueState", classOf[Double])
  )

  override def flatMap(in: sendor, collector: Collector[(String, Double, Double)]) = {
    val lastTemp = lastTempState.value()
    var boolean = false
    //跟最新的温度值求差值做比较
    val diff = (in.temperature - lastTemp).abs
    if(boolean){
      boolean = true
      lastTempState.update(in.temperature)
    }
    if( diff > d){
      collector.collect((in.id, lastTemp, in.temperature))
    }

    //输入最新的温度值
    lastTempState.update(in.temperature)
  }
}
