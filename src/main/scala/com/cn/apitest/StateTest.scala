package com.cn.apitest

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector
import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode


object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    env.setStateBackend( new MemoryStateBackend() )
    env.setStateBackend( new FsStateBackend("") )
//    env.setStateBackend( new RocksDBStateBackend("url")() )

    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)  // 最多允许并行执行的checkpoints数，默认1个
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500) // 两个checkpoint之间的间隔时间至少500毫秒，配了这个上面那个默认就是1了
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)  // 默认false 是否优先使用checkpoint
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)  // 容忍checkpoint失败次数

    // 重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000L))
    env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10,TimeUnit.SECONDS)))

    // 读取数据
    //    val inputPath = "D:\\FlinkLearn\\src\\main\\resources\\sensor.txt"
    //    val inputStream = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("localhost", 7777)  // nc -L -p 7777 windows版

    // 先转换成样例类类型(简单转换操作)
    val dataStream = inputStream
      .map( data => {
        val  arr  = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      }).uid("1")

    // 需求：对于温度传感器温度值跳变，超过10度，报警
    val alertStream = dataStream
      .keyBy(_.id)
//      .flatMap( new TempChangeAlter(10.0) ) // 方法1
      .flatMapWithState[(String, Double, Double), Double]({
        case (data: SensorReading, None) => (List.empty, Some(data.temperature))
        case (data: SensorReading, lastTemp:Some[Double]) => {

          // 跟最新的温度值求差值作比较
          val diff = (data.temperature - lastTemp.get).abs
          if(diff > 10.0)
            ( List((data.id, lastTemp.get, data.temperature)),Some(data.temperature) )
            else
            ( List.empty, Some(data.temperature) )
        }
      } )  // 方法2

    alertStream.print()


    env.execute("state_test")
  }
}

// 实现自定义RichFlatmapFunction
class TempChangeAlter(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义状态保存上一次的温度值
  lazy  val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    // 获取上次的温度值
    val lastTemp = lastTempState.value()
    // 跟最新的温度值求差值作比较
    val diff = (value.temperature - lastTemp).abs
    if(diff > threshold)
      out.collect( (value.id, lastTemp, value.temperature) )

    // 更新状态
    lastTempState.update(value.temperature)
  }
}

// Keyed state测试：必须定义在RichFunction中，因为需要运行时上下文
class MyRichMapper1 extends RichMapFunction[SensorReading, String]{

  var valueState: ValueState[Double] = _
  lazy val listState: ListState[Int] = getRuntimeContext.getListState( new ListStateDescriptor[Int]("liststate",classOf[Int]) )
  lazy val mapState: MapState[String, Double] = getRuntimeContext.getMapState( new MapStateDescriptor[String, Double]("mapstate", classOf[String], classOf[Double]))
  lazy val reduceState: ReducingState[SensorReading] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading]("reducestate",new MyReducer, classOf[SensorReading]))

  override def open(parameters: Configuration): Unit = {
    valueState = getRuntimeContext.getState( new ValueStateDescriptor[Double]("valuestate",classOf[Double]))
  }




  override def map(value: SensorReading): String = {
    // 状态的读写
    val  myV = valueState.value()
    valueState.update(value.temperature)
    listState.add(1)
    val list = new util.ArrayList[Int]()
    list.add(2)
    list.add(3)
    listState.addAll(list)
    listState.update(list)
    listState.get()

    mapState.contains("sensor_1")
    mapState.get("sensor_1")
    mapState.put("sensor_1", 1.3)  // 更新

    reduceState.get()
    reduceState.add(value)

    value.id
  }
}