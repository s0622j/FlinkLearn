package com.cn.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FileOutputTest {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2.连接外部系统，读取数据，注册表
    // 2.1 读取文件
    val filePath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    tableEnv.connect( new FileSystem().path(filePath) )
      //      .withFormat( new OldCsv() )
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // 3.转换操作
    val sensorTable = tableEnv.from("inputTable")
    // 3.1 简单转换
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id)  // 基于id分组
      .select('id, 'id.count as 'count)


    // 4. 输出到文件
    // 注册输出表
    val outputPath = "D:\\FlinkLearn\\src\\main\\resources\\outputsensor.txt"
    tableEnv.connect( new FileSystem().path(outputPath) )
      //      .withFormat( new OldCsv() )
      .withFormat( new Csv() )
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
//        .field("cnt", DataTypes.BIGINT())
        .field("temp2333", DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable")

    resultTable.insertInto("outputTable")
//    aggTable.insertInto("outputTable")  // 文件系统不支持 测试

//    resultTable.toAppendStream[(String, Double)].print("result")
//    aggTable.toRetractStream[(String, Long)].print("agg")

    env.execute("FileOutputTest")

  }

}
