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


    /*
    【步骤二】：
    先检索出人群规则表（swift:dim_crowd_src）的所有数据，
    遍历list，
    list里的起止时间（startTime、endTime）
    能覆盖kafka标签结果数据的订单时间（orderTime）的留下，不能覆盖的滤掉*/
    //    input：
    //（1）kafka数据（标签结果数据）
    val kafkaStream: DataStream[String] = env.fromElements(MockObj.getLabelResult())
    //    kafkaStream.print()

    //（2）HBASE数据（人群规则数据）
    val crowdList: List[String] = MockObj.getCrowdRuleList()
    println("hbase 全量 人群规则数据:")
    crowdList.foreach(println)


    kafkaStream.map(
      kafkaData => {
        val kafkaJsonObject: JSONObject = JSONUtil.parseObj(kafkaData)
        val orderTime: String = kafkaJsonObject.getJSONObject("matchInfo").getStr("orderTime")
        val userCode: String = kafkaJsonObject.getJSONObject("matchInfo").getStr("userCode")
        println(userCode)

        // output：
        //（1）人群规则数据LIST
        val newCrowdList: List[String] = crowdList.filter(
          item => {
            val hbaseRowObj: JSONObject = JSONUtil.parseObj(item)
            val startTime: String = hbaseRowObj.getStr("startTime")
            val endTime: String = hbaseRowObj.getStr("endTime")

            (orderTime >= startTime) && (orderTime <= endTime)
          }
        )

        /*
        【步骤三】：
        rowkey规则：
        遍历[步骤二]输出的人群规则LIST，对list中每一条人群规则的JSON字符串，解析出其中的"crowdCode"，
        再对kafka当前这一帧数据中的"userCode"，按照：MD5(userCode) + _ + crowdCode 规则进行拼接，
        形成rowkey
         */
        //        input：
        // （1）[步骤二的output]人群规则LIST
        val finalCrowList: List[String] = newCrowdList.filter(
          item => {
            val hbaseRowObj: JSONObject = JSONUtil.parseObj(item)
            val crowdCode: String = hbaseRowObj.getStr("crowdCode")
            // （2）按指定rowkey，从人群结果Hbase表(swift:dws_crowd_sink)中查出来的数据
            val rowkey: String = makeMD5str(userCode) + "_" + crowdCode
            //            val rowkey1: String = generateRowKey(userCode, crowdCode)
            //            println(rowkey)

            val connectionToList: Connection => util.List[String] = getHDataByRowKey(_, "dws_crowd_sink", "swift", "", rowkey)
            connectionToList != null
          }
        )

        //        output：
        //（1）人群规则LIST（去重后的list）
        finalCrowList

        /*
        【步骤四】：
        rowkey规则：
        对kafka中标签结果JSON数据进行解析，得到"userCode"，对其取MD5作为rowkey

        使用该rk查询HBASE表(swift:dws_label_sink)获得该用户的所有标签结果的LIST
         */

        //        input：
        //（1）kafka中标签结果数据
        val rowKey: String = makeMD5str(userCode)
        //（2）HBASE中标签结果数据
        val finalLabelList: Connection => util.List[String] = getHDataByRowKey(_, "dws_label_sink", "swift", "", rowKey)
        //        output：
        //（1）"userCode"对应的标签结果数据LIST（Map[usercode,List[String]]）
        //        val result4: (String, Connection => util.List[String]) = (userCode, labelList)

        /*
        【步骤五】：
        过滤规则：
        外层遍历[步骤三的输出]人群规则LIST中的每个人群规则，对标签结果列表，进行预过滤：
        内层遍历[步骤四的输出]标签结果LIST中的每个标签结果，
        选取标签结果数据："orderTime"
        在人群规则数据："startTime" 和 "endTime"
        时间范围内的标签结果数据，留下，不在时间范围内的标签结果数据被舍弃，
        从而生成过滤后的标签结果LIST
         */

        //        input：
        //（1）[步骤三的输出]人群规则LIST finalCrowList
        //（2）[步骤四的输出]标签结果LIST result4
        //创建一个空的可变列表
        finalCrowList.foreach(
          item => {
            val hbaseRowObj: JSONObject = JSONUtil.parseObj(item)
            val startTime: String = hbaseRowObj.getStr("startTime")
            val endTime: String = hbaseRowObj.getStr("endTime")

            val labelListResult: List[Connection => util.List[String]] = List(finalLabelList).filter(
              elem => {
                val labelJsonObject: JSONObject = JSONUtil.parseObj(elem)
                val labelOrderTime: String = labelJsonObject.getJSONObject("matchInfo").getStr("orderTime")

                (orderTime >= startTime) && (orderTime <= endTime)
              }
            )
            //        output：
            //（1）标签结果LIST（过滤后的list）
            labelListResult
          }
        )

      }
    )










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
    //    if (true) { // 逻辑判断决定sink对象的不同，自定义ricksink中(invoke)区分逻辑
    //      dataStream.addSink(new MyHBaseSink("test_htbl"))
    //    } else {
    //      dataStream.addSink(new MyHBaseSink("test_htbl"))
    //    }ou

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
