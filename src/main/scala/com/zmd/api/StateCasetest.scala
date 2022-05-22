package com.zmd.api

import org.apache.flink.streaming.api.scala._

//对于温度传感器温度值跳变超过10度报警

object StateCasetest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val inputstream: DataStream[String] = environment.socketTextStream("localhost", 7777)

    val dataStream: DataStream[sendor] = inputstream.map(date => {
      val arr: Array[String] = date.split(",")
      sendor(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    val alertStream: DataStream[(String, Double, Double)] = dataStream
      .keyBy((_: sendor).id)
      //.flatMap(new TempChangeAlert(10.0))
      //Option
      .flatMapWithState[(String, Double, Double),Double]( {
        case (data: sendor, None) => (List.empty, Some(data.temperature) )
        case (data: sendor, lastTemp: Some[Double]) => {
          val diff: Double = (data.temperature - lastTemp.get).abs
          if( diff > 10.0){
            (List((data.id, lastTemp.get, data.temperature)), Some(data.temperature) )
          }
          else{
            (List.empty, Some(data.temperature))
          }
        }
      })
    alertStream.print("alertStream")

    environment.execute()
  }
}
