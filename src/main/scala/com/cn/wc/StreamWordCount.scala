package com.cn.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    //创建流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(8)

    env.disableOperatorChaining() //全局禁用任务链

    //从外部命令中提取参数，作为socket主机名和端口号
    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //接收一个socket文本流
    // nc -lk 7777
    // nc -L -p 7777 windows版
    val inputDataStream: DataStream[String] = env.socketTextStream(host,port).slotSharingGroup("b")
//    val inputDataStream: DataStream[String] = env.socketTextStream("localhost",7777)

    //进行转换处理统计
    val resultDataStream: DataStream[(String,Int)] = inputDataStream
      .flatMap(_.split(" ")).slotSharingGroup("a") //实现独享一个slot组a 默认一般是default组不定义这个就行
      .filter(_.nonEmpty).slotSharingGroup("b")//.disableChaining()  将任务链前后都拆开，不参与任务链的合并（1分3）
      .map((_,1))//.startNewChain() 开启一个新的任务链 （一分为二）  //.setParallelism(3)
      .keyBy(0)
      .sum(1)//.setParallelism(2)

    resultDataStream.print().setParallelism(1)

    //启动任务执行
    env.execute("Stream_Word_Count")
  }
}

//xx test0714
