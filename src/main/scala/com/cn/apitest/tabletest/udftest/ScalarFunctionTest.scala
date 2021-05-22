package com.cn.apitest.tabletest.udftest

import com.cn.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {

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

    // 调用自定义hash函数，对id进行hash运算
    // 1.table api
    // 首先new一个UDF的实例
    val hashCode = new HashCode(23)
    val resultTable = sensorTable
      .select('id, 'ts, hashCode('id))

    // 2.sql
    // 需要在环境中注册UDF
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode", hashCode)
    val resuleSqlTable = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor")

    resultTable.toAppendStream[Row].print("result")
    resuleSqlTable.toAppendStream[Row].print("sql")

    env.execute("scalar_function_test")

  }

}


// 自定义标量函数
class HashCode(factor: Int) extends ScalarFunction{
  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }
}