package com.hypers.yum.entrance

import cn.hutool.json.{JSONObject, JSONUtil}
import com.hypers.yum.rich.sinks.MyHBaseSink
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import com.hypers.yum.test.mock.MockObj
import com.hypers.yum.util.HBaseUtil._
import com.hypers.yum.util.Tools.makeMD5str
import org.apache.hadoop.hbase.client.Connection

import scala.collection.mutable.ListBuffer
import java.util
import scala.collection.mutable

/**
 * @Author 4
 * @Description //TODO 程序入口
 * @Date 2021/12/9
 *       PS:
 *       hosts --> 10.16.3.100  master01.rose.cn    master01
 * */
object MainEntrance {

  def main(args: Array[String]): Unit = {


    /*
     1.获取一个执行环境（execution environment）
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // checkpoint every 5000 msecs
    env.enableCheckpointing(5000)
    // 设置并发度
    env.setParallelism(6)
    // 解析运行参数
    val paraTool = ParameterTool.fromArgs(args)
    // get label_file_path
    val label_file_path = paraTool.get("label_file_path")

    /*
     2.加载/创建初始数据
     */
    //    val dataStream: DataStream[String] = env.readTextFile(label_file_path)
    val kafkaStream: DataStream[String] = env.fromElements(MockObj.getLabelResult())
    //    kafkaStream.print()


    // hbase data
    val crowdList: List[String] = MockObj.getCrowdRuleList()

    println("start:")
    crowdList.foreach(println)


    //创建一个空的可变列表
    var newCrowdListBuffer = new ListBuffer[String]

    val newSteam: DataStream[Unit] = kafkaStream.map(
      kafkaData => {
        val kafkaJsonObject: JSONObject = JSONUtil.parseObj(kafkaData)
        val orderTime: String = kafkaJsonObject.getJSONObject("matchInfo").getStr("orderTime")
        //        println(orderTime)

        val newCrowdList: List[String] = crowdList.filter(
          item => {
            val hbaseRowObj: JSONObject = JSONUtil.parseObj(item)
            val startTime: String = hbaseRowObj.getStr("startTime")
            val endTime: String = hbaseRowObj.getStr("endTime")
            // 打印时间
            //            println(startTime, endTime, orderTime)
            // 打印逻辑的结果
            //            println((orderTime >= startTime) && (orderTime <= endTime))
            // 过滤掉不满足条件的数据
            (orderTime >= startTime) && (orderTime <= endTime)
          }
        )
        println("end:")
        //        newCrowdList.foreach(println)

        newCrowdListBuffer = newCrowdList.to[ListBuffer]
        newCrowdListBuffer.foreach(println)
      }
    )

    newCrowdListBuffer.foreach(println)









    /*
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "10.16.3.100:9092")
    kafkaProps.setProperty("group.id", "test_grouplabel")

//    println("topic:\t" + paraTool.get("topic"))

    val flinkKafkaConsumer = new FlinkKafkaConsumer[java.lang.String](
      paraTool.get("topic"),
      // 这里不使用JsonDeserializationSchema，
      // 由于 flinkKafkaConsumer 的容错能力，
      // 在损坏的消息上失败作业将使 flinkKafkaConsumer 尝试再次反序列化消息.
      // 因此，如果反序列化仍然失败，则 flinkKafkaConsumer 将在该损坏的消息上进入不间断重启和失败的循环
      new SimpleStringSchema,
      //        paraTool.getProperties
      kafkaProps
    )


    // 测试时使用，尽可能从最早的记录开始消费，在该模式下，Kafka 中的 committed offset 将被忽略，不会用作起始位置
    flinkKafkaConsumer.setStartFromEarliest()

    val dataStream = env.addSource(flinkKafkaConsumer)
     */


    /*
     3.指定数据相关的转换
     */


    /*
     4.指定计算结果的存储位置
     */
    //    dataStream.print()
    //    if (true) { // 逻辑判断决定sink对象的不同，自定义ricksink中(invoke)区分逻辑
    //      dataStream.addSink(new MyHBaseSink("test_htbl"))
    //    } else {
    //      dataStream.addSink(new MyHBaseSink("test_htbl"))
    //    }

    /*
    val myKafkaProducer = new FlinkKafkaProducer[String](
      "my-topic", // 目标 topic
      new SimpleStringSchema(), // 序列化 schema
      kafkaProps, // producer 配置
      FlinkKafkaProducer.Semantic.EXACTLY_ONCE) // 容错

    dataStream.addSink(myKafkaProducer)
     */


    /*
     5.触发程序执行
     */
    env.execute("test")


  }


}
