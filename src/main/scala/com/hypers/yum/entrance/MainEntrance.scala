package com.hypers.yum.entrance

import com.hypers.yum.rich.sinks.MyHBaseSink
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

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
    if (true) { // 逻辑判断决定sink对象的不同，自定义ricksink中(invoke)区分逻辑
      dataStream.addSink(new MyHBaseSink("test_htbl"))
    } else {
      dataStream.addSink(new MyHBaseSink("test_htbl"))
    }




    /*
     5.触发程序执行
     */
    env.execute("test")


  }


}
