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
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // 3.转换操作
    val sensorTable = tableEnv.from("inputTable")
    val resultTable





  }

}
