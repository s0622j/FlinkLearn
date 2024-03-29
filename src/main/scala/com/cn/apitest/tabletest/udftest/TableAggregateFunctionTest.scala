package com.cn.apitest.tabletest.udftest


import com.cn.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object TableAggregateFunctionTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 读取数据
    //    val inputStream = env.socketTextStream("localhost", 7777)  // nc -L -p 7777 windows版
    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)
    // 先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map(data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)


    // table api
    val top2Temp = new Top2Temp()
    val resultTable = sensorTable
      .groupBy('id)
      .flatAggregate( top2Temp('temperature) as ('temp, 'rank) )
      .select('id, 'temp, 'rank)

    // sql 自带函数

    resultTable.toRetractStream[Row].print()

    env.execute("table_aggregate_function_test")

  }

}

// 定义一个类用来表示表聚合函数的状态
class Top2TempAcc{
  var highestTemp:Double = Double.MinValue
  var secondHighestTemp: Double = Double.MinValue
}

// 自定义表聚合函数，提取所有温度值中最高的两个温度,输出(temp, rank)
class  Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc]{
  override def createAccumulator(): Top2TempAcc = new Top2TempAcc()

  // 实现计算聚合结果的函数accumulate
  def accumulate(acc : Top2TempAcc, temp:Double) = {
    // 判断当前温度值，是否比状态中的大
    if( temp > acc.highestTemp){
      // 如果比最高温度还高，排在第一，原来的第一顺到第二位
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if(temp > acc.secondHighestTemp){
      // 如果在最高和第二高之间，那么直接替换第二高温度
      acc.secondHighestTemp = temp
    }
  }

  // 实现一个输出结果的方法，最终处理完表中所有数据时调用
  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit ={
    out.collect((acc.highestTemp, 1))
    out.collect((acc.secondHighestTemp,2))
  }
}
