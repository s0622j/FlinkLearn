package com.cn.apitest

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStateBackend(new FsStateBackend(""))
    env.setStateBackend(new RocksDBStateBackend("url"))


    // 读取数据

    val inputStream = env.socketTextStream("localhost", 7777)  // nc -L -p 7777 windows版

    // 先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map( data => {
        val  arr  = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    val highTempStream = dataStream
        .process( new SplitTempProcessor(30.0) )

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String, Long, Double)]("low")).print("low")

    env.execute("side_output_test")
  }

}

// 实现自定义的ProcessFunction,进行分流
class  SplitTempProcessor(threshold: Double) extends  ProcessFunction[SensorReading, SensorReading]{
  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {

    if ( value.temperature > threshold ){
      // 如果当前温度值大于30，那么输出到主流
      out.collect(value)
    } else {
      // 如果不超过30度，那么输出到测输出流
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temperature))
    }
  }
}