package com.cn.wc

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)

    //接收一个socket文本流
    // nc -lk 7777
    // nc -L -p 7777 windows版
    val inputDataStream: DataStream[String] = env.socketTextStream("localhost",7777)
    //进行转换处理统计
    val resultDataStream: DataStream[(String,Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)

    resultDataStream.print()

    //启动任务执行
    env.execute("Stream_Word_Count")
  }
}
