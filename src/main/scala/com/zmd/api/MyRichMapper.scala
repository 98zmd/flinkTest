package com.zmd.api

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

class MyRichMapper extends RichMapFunction[sendor, String] {
  var valueState: ValueState[Double] = _
  lazy val ListState: ListState[Int] = getRuntimeContext.getListState(
    new ListStateDescriptor[Int]("Liststate", classOf[Int]))
  lazy val mapState: MapState[String, Double] =  getRuntimeContext.getMapState(
    new MapStateDescriptor[String, Double]("mapState",classOf[String],classOf[Double])
  )
  lazy val reduceState: ReducingState[sendor] = getRuntimeContext.getReducingState(
    new ReducingStateDescriptor[sendor]("reducing State",new MyReducer ,classOf[sendor])
  )
  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }
  override def map(in: sendor): String = {
    val myv: Double = valueState.value()
    valueState.update(in.temperature)

    in.id
  }
}
