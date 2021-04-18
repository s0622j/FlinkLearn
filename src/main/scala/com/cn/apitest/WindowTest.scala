package com.cn.apitest

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map( data => {
        val  arr  = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val resultStream = dataStream
      .map( data => (data.id, data.temperature) )
//      .keyBy(data => data._1)
      .keyBy(_._1)  // 按照二元组的第一个元素（id）分组
//      .window( TumblingEventTimeWindows.of(Time.seconds(15)) )  // 滚动时间窗口
//      .window( SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3)) )  // 滑动时间窗口
//      .window( EventTimeSessionWindows.withGap(Time.seconds(10)) )  // 会话窗口
      .timeWindow( Time.seconds(15) )
//      .countWindow( 10 )
  }
}
