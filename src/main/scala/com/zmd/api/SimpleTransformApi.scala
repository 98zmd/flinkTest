package com.zmd.api

import org.apache.flink.streaming.api.scala._

object SimpleTransformApi {

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val inputPath = "/Users/zmd/IdeaProjects/flinkTest/src/main/resources/data.txt"
    val inputStream: DataStream[String] = environment.readTextFile(inputPath)

    val dataStream: DataStream[sendor] = inputStream
      .map(
        data => {
          val array = data.split(", ")
          sendor(array(0), array(1).toLong, array(2).toDouble)
    })

    //输出最小的温度值 最近时间
    val myReduceMin: DataStream[sendor] = dataStream
      .keyBy("id")
      .reduce(
        (currentState, newData) =>
          sendor(currentState.id, newData.timestamp, currentState.temperature.min(newData.temperature))
      )
    //val result: DataStream[sendor] = dataStream.keyBy("id").min("temperature")

    //result.print()
    myReduceMin.print()
    //分流操作  低温 高温 分流
    val splitStream: SplitStream[sendor] = dataStream.split(data => {
      if (data.temperature > 30.0) {
        Seq("high")
      } else {
        Seq("low")
      }
    })
    val highStream: DataStream[sendor] = splitStream.select("high")
    val lowStream: DataStream[sendor] = splitStream.select("low")
    val allStream: DataStream[sendor] = splitStream.select("high", "low")

    //highStream.print("high")
    //lowStream.print("low")
    //allStream.print("all")

    //合流 connect comap
    val warningStream: DataStream[(String, Double)] = highStream.map(data => (data.id, data.temperature))
    val connectStream: ConnectedStreams[sendor, (String, Double)] = lowStream.connect(warningStream)

    //coMap
    val coMapStream: DataStream[(String, Double, String)] = connectStream.map(
      data => {
        (data.id, data.temperature, "normal")
      },
      data1 => {
        (data1._1, data1._2, "waring")
      }
    )

    val all: DataStream[sendor] = highStream.union(lowStream)
    all.print("all")

    coMapStream.print("coMapStream")
    environment.execute("test")
  }
}
