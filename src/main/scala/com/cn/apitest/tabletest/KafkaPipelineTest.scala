package com.cn.apitest.tabletest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipelineTest {
  def main(args: Array[String]): Unit = {
    // 1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv = StreamTableEnvironment.create(env)

    // 2 从kafka读取数据
    /**
      * 启动kafka控制台生产者
      * /soft/kafka/bin/kafka-server-start.sh -daemon /soft/kafka/config/server.properties
      * [root@data101 /root]#cd /soft/kafka
      * ./bin/kafka-console-producer.sh --broker-list data101:9092 --topic sensor
      *
      * * 启动一个kafka消费者看是否写入
      * * cd /soft/kafka
      * * ./bin/kafka-console-consumer.sh --bootstrap-server data101:9092 --topic sinktest --zookeeper master100:2181
      */
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

    // 3.查询转换
    // 3.1 简单转换
    val sensorTable = tableEnv.from("kafkaInputTable")
    val resultTable = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable = sensorTable
      .groupBy('id)  // 基于id分组
      .select('id, 'id.count as 'count)

    // 4. 输出到kafka
    tableEnv.connect( new Kafka()
      .version("0.11")
      .topic("sinktest")
      .property("zookeeper.connect", "data102:2181")
      .property("bootstrap.servers", "data101:9092")
    )
      .withFormat(new Csv)
      .withSchema( new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")


    env.execute("kafka_pipeline_test")

  }

}
