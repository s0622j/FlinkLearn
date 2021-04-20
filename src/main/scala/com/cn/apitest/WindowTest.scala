package com.cn.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
//    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
//    val inputStream = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map( data => {
        val  arr  = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 每15秒统计一次，窗口内各传感器所有温度的最小值,以及最新的时间戳
    val resultStream = dataStream
      .map( data => (data.id, data.temperature, data.timestamp) )
//      .keyBy(data => data._1)
      .keyBy(_._1)  // 按照二元组的第一个元素（id）分组
//      .window( TumblingEventTimeWindows.of(Time.seconds(15)) )  // 滚动时间窗口
//      .window( SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3)) )  // 滑动时间窗口
//      .window( EventTimeSessionWindows.withGap(Time.seconds(10)) )  // 会话窗口
//      .countWindow( 10 )  // 滚动计数窗口
      .timeWindow( Time.seconds(15) )
//      .minBy(1)
      .reduce( (curRes, newData) => (curRes._1, curRes._2.min(newData._2), newData._3) )

    resultStream.print()
    env.execute("window_test")
  }
}

class  MyReducer extends ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
  }
}