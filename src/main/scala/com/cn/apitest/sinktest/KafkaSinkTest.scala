package com.cn.apitest.sinktest

import java.util.Properties

import com.cn.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    // 从kafka读取数据
    /**
      * 启动kafka控制台生产者
      * [root@data101 /root]#cd /soft/kafka
      * ./bin/kafka-console-producer.sh --broker-list data101:9092 --topic sensor
      */
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "data101:9092")
    properties.setProperty("group.id", "consumer-group")
    val stream = env.addSource( new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties) )

    // 先转换成样例类类型(简单转换操作)
//    val dataStream = inputStream
    val dataStream = stream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      } )

    // 写入kafka生产者
    /**
      * 启动一个kafka消费者看是否写入
      * cd /soft/kafka
      * ./bin/kafka-console-consumer.sh --bootstrap-server data101:9092 --topic sinktest --zookeeper master100:2181
      */
    dataStream.addSink( new FlinkKafkaProducer011[String]("data101:9092","sinktest", new SimpleStringSchema()) )

    env.execute("kafka_sink_test")
  }
}
