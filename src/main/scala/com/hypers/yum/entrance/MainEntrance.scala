package com.hypers.yum.entrance

import com.hypers.yum.rich.sinks.MyHBaseSink
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, HTable, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.util.Properties

/**
 * PS:
 * hosts --> 10.16.3.100  master01.rose.cn    master01
 */
object MainEntrance {


  def main(args: Array[String]): Unit = {


    /*
     1.获取一个执行环境（execution environment）
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置并发度
    env.setParallelism(6)
    // 解析运行参数
    val paraTool = ParameterTool.fromArgs(args)
    // get label_file_path
    val label_file_path = paraTool.get("label_file_path")

    /*
     2.加载/创建初始数据
     */
    val dataStream: DataStream[String] = env.readTextFile(label_file_path)


    /*
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "10.16.3.100:9092")
    kafkaProps.setProperty("group.id", "test_grouplabel")

//    println("topic:\t" + paraTool.get("topic"))

    val flinkKafkaConsumer = new FlinkKafkaConsumer[java.lang.String](
      paraTool.get("topic"),
      new SimpleStringSchema,
      //        paraTool.getProperties
      kafkaProps
    )

    flinkKafkaConsumer.setStartFromEarliest()  // 尽可能从最早的记录开始消费

    val dataStream = env.addSource(flinkKafkaConsumer)
     */




    /*
     3.指定数据相关的转换
     */


    /*
     4.指定计算结果的存储位置
     */
//    dataStream.print()

    dataStream.addSink(new MyHBaseSink("test_htbl"))



    /*
     5.触发程序执行
     */
    env.execute("test")


  }


}
