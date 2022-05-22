package com.zmd.api

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties
import scala.tools.cmd.Property

case class sendor(id:String,
                  timestamp: Long,
                  temperature: Double,
                 )

object ApiEnv {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val data = List(
      sendor("sensor_1", 1547718199, 35.8),
      sendor("sensor_6", 1547718201, 15.4),
      sendor("sensor_7", 1547718202, 6.7),
      sendor("sensor_10", 1547718205, 38.1)
    )

    val streamdata = env.fromCollection(data)
    streamdata.print()

    val inputPath = "/Users/zmd/IdeaProjects/flinkTest/src/main/resources/data.txt"
    val textData = env.readTextFile(inputPath)
    textData.print()


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "device1:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    //从Kafka里面读取数据
    val value = env.addSource(new FlinkKafkaConsumer011[String]("first", new SimpleStringSchema(), properties))
    value.print()

    //自定义source
    val streamDIY = env.addSource(new MyFlinkSource())
    streamDIY.print()

    env.execute()

  }
}
