package com.cn.apitest.tabletest

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.table.types.DataType

object TableApiTest {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    /*
    // 1.1基于老版本planner的流处理
    val settings = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)

    // 1.2 基于老版本的批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    // 1.3 基于blink planner的流处理
    val blinkStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

    // 1.4 基于blink planner的批处理
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings)
    */

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

//    val inputTable: Table = tableEnv.from("inputTable")
//    inputTable.toAppendStream[(String, Long, Double)].print()

    // 2.2 从kafka读取数据
    tableEnv.connect( new Kafka()
        .version("0.11")
        .topic("sensor")
        .property("zookeeper.connect", "data102:2181")
        .property("bootstrap.servers", "data101:9092")
    )
        .withFormat(new Csv)
        .withSchema( new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
        )
        .createTemporaryTable("kafkaInputTable")
    val inputTable: Table = tableEnv.from("kafkaInputTable")
    inputTable.toAppendStream[(String, Long, Double)].print()

    // 3.查询转换
    //  3.1使用table api
    val sensorTable = tableEnv.from("inputTable")
    val resultTable = sensorTable
        .select('id, 'temperature)
        .filter('id === "sensor_1")

    // 3.2 SQL
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,temperature
        |from inputTable
        |where id = 'sensor_1'
      """.stripMargin)
    resultTable.toAppendStream[(String,Double)].print("result")
    resultSqlTable.toAppendStream[(String,Double)].print("sql")

    env.execute("table_api_test")


  }

}
