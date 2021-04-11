package com.cn.apitest.sinktest

import java.util

import com.cn.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
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

    // 定义HttpHost
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 自定义写入es的EsSinkFunction
    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        // 包装一个Map作为 data source
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)

        // 创建index request， 用于发送http请求
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        // 用indexer发送请求
        requestIndexer.add(indexRequest)

      }
    }

    dataStream.addSink( new ElasticsearchSink
        .Builder[SensorReading](httpHosts, myEsSinkFunc)
        .build()
    )

    env.execute("es_sink_test")
  }
}
