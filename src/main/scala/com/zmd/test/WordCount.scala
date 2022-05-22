package com.zmd.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.createTypeInformation

object WordCount {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val inputPath = "/Users/zmd/IdeaProjects/flinkTest/src/main/resources/word.txt"
    val inputDS = env.readTextFile(inputPath)

    //分词，对单词进行分组，然后sum聚合
    val wordCountDS = inputDS.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    //打印输出
    wordCountDS.print()

  }
}
