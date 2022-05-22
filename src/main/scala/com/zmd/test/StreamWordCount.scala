package com.zmd.test

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //从外部获取信息的参数
    val host = "localhost"
    val port = 7777

    //创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //接受socket文本流
    val textDstream = env.socketTextStream(host, port)

    val dataStream = textDstream
      .flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    dataStream.print().setParallelism(1)

    env.execute("Socket Word Count")
  }

}
