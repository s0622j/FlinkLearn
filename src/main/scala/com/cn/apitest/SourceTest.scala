package com.cn.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random


// 定义传感器数据样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {

  def main(args: Array[String]): Unit = {

    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 从集合中读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    )

    val stream1 = env.fromCollection(dataList)

//    env.fromElements(1.0,33,"hh")

    // 2. 从文件中读取数据
    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    val stream2 = env.readTextFile(inputPath)

//    stream1.print()
//    stream2.print()


    // 3. 从kafka中读取数据
    /**
      * 启动kafka控制台生产者
      * /soft/kafka/bin/kafka-server-start.sh -daemon /soft/kafka/config/server.properties
      * [root@data101 /root]#cd /soft/kafka
      * ./bin/kafka-console-producer.sh --broker-list data101:9092 --topic sensor
      */
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "data101:9092")
    properties.setProperty("group.id", "consumer-group")
    val stream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties) )

//    stream3.print()

    // 4. 自定义Source数据源
    val stream4 = env.addSource( new MySensorSource() )

    stream4.print()


    //执行
    env.execute("source_test")

  }

}

// 自定义SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{
  // 定义一个标识位flag，用来标识数据源是否正常运行发出数据
  var running: Boolean = true
  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 定义一个随机数发生器
    val rand = new Random()
    // 随机生成一组（10个）传感器的初始温度：（id, temp）
    var curtTemp = 1.to(10).map( i => ("sensor" + i, rand.nextDouble() * 100 ) )

    // 定义无限循环，不停地产生数据，除非被cancel
    while(running){
      // 在上次数据基础上微调，更新温度值
      curtTemp = curtTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前时间戳，加入到数据中，调用ctx.collect发出数据
      val curTime = System.currentTimeMillis()
      curtTemp.foreach(
        data => ctx.collect(SensorReading(data._1, curTime, data._2))
      )
      // 间隔500ms
      Thread.sleep(500)
    }
  }
}
