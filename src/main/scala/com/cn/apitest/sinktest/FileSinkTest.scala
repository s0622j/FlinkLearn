package com.cn.apitest.sinktest

import com.cn.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FileSinkTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      } )

    dataStream.print()
//    dataStream.writeAsCsv("D:\\FlinkLearn\\src\\main\\resources\\out.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path("D:\\FlinkLearn\\src\\main\\resources\\out1.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("file_sink_test")
  }
}
