package com.cn.apitest

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object TransformTest {

  def main(args: Array[String]): Unit = {

    // 0.读取数据
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 1.先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      } )
//      .filter(new MyFilter)
    // 2.分组聚合，输出每个传感器当前最小值()
    val aggStream = dataStream
      .keyBy("id")  // 根据id进行分组 // 新版本keyBy("id")过时了，用keyBy(_.id)替代
//      .min("temperature")
      .minBy("temperature")

//    aggStream.print()

    // 3.需要输出当前最小温度值，以及最近的时间戳，要用reduce
    val resultStream = dataStream
        .keyBy("id")
//        .reduce( (curState,newData) =>  // reduce是对每两个数据进行处理
//        SensorReading( curState.id, newData.timestamp, curState.temperature.min(newData.temperature) )
//        )
        .reduce(new MyReduceFunction)

//    resultStream.print()

    // 4.多流转换操作
    // 4.1 分流，将传感器温度数据分成低温、高温两条流
    val splitStream = dataStream
        .split( data => {
          if ( data.temperature > 30.0 ) Seq("high") else Seq("low")
        } )
    val highTempStream = splitStream.select("high")
    val lowTempStream = splitStream.select("low")
    val allTempStream = splitStream.select("high","low")

//    highTempStream.print("high")
//    lowTempStream.print("low")
//    allTempStream.print("All")

    // 4.2 合流，connect
    val warningStream = highTempStream.map( data => (data.id, data.temperature) )
    val connectedStreams = warningStream.connect(lowTempStream)  // 合并连接

    // 用coMap对数据进行分别处理
    val coMapResultStream = connectedStreams
        .map(
          waringData => (waringData._1,waringData._2, "warning") ,
          lowTempData => (lowTempData.id, "healthy")

        )

    // 4.3 union合流 数据类型要匹配
    val unionStream = highTempStream.union(lowTempStream, allTempStream)

    coMapResultStream.print("coMap")

    env.execute("transform_test")

  }
}

class MyReduceFunction extends  ReduceFunction[SensorReading]{
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
}


// 自定义一个函数类
class  MyFilter extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean =
    value.id.startsWith("sensor_1")
}

class MyMapper extends MapFunction[SensorReading, String]{
  override def map(value: SensorReading): String = ???
}

// 富函数，可以获取到运行时上下文，还有一些生命周期方法
class MyRichMapper extends RichMapFunction[SensorReading, String]{

  override def open(parameters: Configuration): Unit = {
    // 做一些初始化操作，比如数据库的连接
    getRuntimeContext
  }

  override def map(value: SensorReading): String = value.id + " temperatuer"

  override def close(): Unit = {
    // 一般做收尾工作，比如关闭数据库连接，或者清空状态
  }
}