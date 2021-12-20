package com.hypers.yum.entrance

import cn.hutool.json.{JSONArray, JSONObject, JSONUtil}
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
      kafkaLabelData => {
        val kafkaJsonObject: JSONObject = JSONUtil.parseObj(kafkaLabelData)
        val orderTime: String = kafkaJsonObject.getJSONObject("matchInfo").getStr("orderTime")
        val userCode: String = kafkaJsonObject.getJSONObject("matchInfo").getStr("userCode")
        //        println(userCode)

        // output：
        //（1）人群规则数据LIST

        val afterTimeFilteringCrowdList: List[String] = crowdList.filter(
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
        去[步骤二]输出的人群规则list做去重：
        人群规则list - Hbase中查出来的有数的
         */
        //        input：
        // （1）[步骤二的output]人群规则LIST
        val afterHbaseFilteringCrowdList: List[String] = afterTimeFilteringCrowdList.filter(
          item => {
            val hbaseRowObj: JSONObject = JSONUtil.parseObj(item)
            val crowdCode: String = hbaseRowObj.getStr("crowdCode")
            // （2）按指定rowkey，从人群结果Hbase表(swift:dws_crowd_sink)中查出来的数据
            val rowkey: String = makeMD5str(userCode) + "_" + crowdCode
            //            val rowkey1: String = generateRowKey(userCode, crowdCode)
            //            println(rowkey)

            val connectionToList: ListBuffer[String] = getHDataByRowKey(null, "swift:dws_crowd_sink", "f1", "crowd", rowkey)
            // 在habse查不到结果的人群规则保留
            connectionToList.isEmpty
          }
        )

        //        output：
        //（1）人群规则LIST（去重后的list）
        afterHbaseFilteringCrowdList

        /*
        【步骤四】：
        rowkey规则：
        对kafka中标签结果JSON数据进行解析，得到"userCode"，对其取MD5作为rowkey
        使用该rk查询HBASE表(swift:dws_label_sink)获得该用户的所有标签结果的LIST
         */

        //        input：
        //（1）kafka中标签结果数据
        val rowKey: String = makeMD5str(userCode)
        //（2）HBASE中标签结果数据,根据前缀查询
        val hbaseLabelList: ListBuffer[String] = getHDataByRowKey(null, "swift:dws_label_sink", "f1", "label", rowKey)
        //        output：
        //（1）"userCode"对应的标签结果数据LIST（Map[usercode,List[String]]）
        hbaseLabelList

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
        //（1）[步骤三的输出]人群规则LIST
        afterHbaseFilteringCrowdList
        //（2）[步骤四的输出]标签结果LIST
        hbaseLabelList

        //创建一个 Map 收集满足时间筛选的 Map(crowcode,LIST(标签结果))
        val finalCrowdLabelMap: mutable.Map[String, ListBuffer[String]] = mutable.Map[String, ListBuffer[String]]()
        //        val afterTimeFilteringHbaseLabelList: ListBuffer[String] = ListBuffer[String]()

        afterHbaseFilteringCrowdList.foreach(
          crowd => {
            val hbaseRowObj: JSONObject = JSONUtil.parseObj(crowd)
            val crowdStartTime: String = hbaseRowObj.getStr("startTime")
            val crowdEndTime: String = hbaseRowObj.getStr("endTime")

            //创建一个 ListBuffer 收集满足时间筛选的 hbaseLabelList
            val tmpLabelListBuffer: ListBuffer[String] = hbaseLabelList.filter(
              elem => {
                val labelJsonObject: JSONObject = JSONUtil.parseObj(elem)
                val labelOrderTime: String = labelJsonObject.getJSONObject("matchInfo").getStr("orderTime")

                (labelOrderTime >= crowdStartTime) && (orderTime <= crowdEndTime)
              }
            )
            //            afterTimeFilteringHbaseLabelList ++= tmpLabelListBuffer
            //            val tmpCrowdLabelMap: mutable.Map[ListBuffer[String], ListBuffer[String]] =
            //            finalCrowdLabelMap ++= mutable.Map(item,tmpLabelListBuffer)
            finalCrowdLabelMap += (crowd -> tmpLabelListBuffer)
          }
        )
        //output：
        //（1）标签结果LIST（过滤后的list）--> 数据结构为：Map(crowcode,LIST(标签结果))
        finalCrowdLabelMap

        /*      【步骤六】：
        转化逻辑：
        Map(crowcode,LIST(标签结果))

        1）人群结果 = 人群规则一些key + kafka中的标签结果的matchinfo；
        2）遍历Map中的每个key，也就是每条人群规则，根据人群规则作用在key对应的value（标签结果LIST）所返回的true/false，来决定当前遍历到的map的key是否留下，
        如果为true，map的key（该条人群规则）留下，否则过滤掉；
        3）如何根据人群规则判断true/false呢？
        "subRules"的叶子节点中：
        {
        "logic": null,--
        "subRules": [],
        "labelId": "label-1"
        }
        "labelId"在"标签结果LIST"中的"labelId"中存在对应值，则该json对象为true，否则为false。
        而没一个叶子节点都计算完true/false后，按照"logic"的取值是"and"或"or"，对所有叶子节点的真假返回值进行取"与"/"或"运算，得到最后的true/false值。
        最后的与或逻辑后获得的true/false值来决定当前MAP的key是否留下还是过滤掉；
        */


        //input：
        //（1）[步骤五的输出]Map(crowcode,LIST(标签结果))
        finalCrowdLabelMap.filter(
          mapElem => {
            val crowdElem: String = mapElem._1
            val labelList: ListBuffer[String] = mapElem._2

            judgeSingleLogic(labelList: ListBuffer[String], crowdElem: String)
          }
        ).map(
          mapElem => {
            //4）如果留下，则该人群结果取一部分key值+从当前kafka的标签结果的matchinfo中取一些值=拼成该人群规则对应的人群结果；
            // 来自hbase 通过逻辑计算的<人群规则> 的字段
            val crowdElem: String = mapElem._1

            // 来自kafka 的<标签结果> 的字段
            kafkaLabelData


            // 转换为Json
            ""
          }
        )


        //5）没一个被留下的map的key最后都拼出了一条人群结果，封装成一个"人群结果LIST"


        //output：
        //（1）人群结果LIST（根据人群规则的与或逻辑进行转化）


      }
    )
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

  //  def getRule(crowdRule: String): String = {
  //    val hbaseRowObj: JSONObject = JSONUtil.parseObj(crowdRule)
  //    val startTime: String = hbaseRowObj.getStr("startTime")
  //    ""
  //  }

  def getLogicType(crowdRule: String): String = {
    val hbaseRowObj: JSONObject = JSONUtil.parseObj(crowdRule)
    val logic: String = hbaseRowObj.getStr("logic")
    logic
  }

  def getSubRules(crowdRule: String): JSONArray = {
    val hbaseRowObj: JSONObject = JSONUtil.parseObj(crowdRule)
    val subRules: JSONArray = hbaseRowObj.getJSONArray("subRules")
    subRules
  }

  def getLabelId(crowdRule: String): String = {
    val hbaseRowObj: JSONObject = JSONUtil.parseObj(crowdRule)
    val labelId: String = hbaseRowObj.getStr("labelId")
    labelId
  }

  //  递归实现:              Map(crowcode,LIST(标签结果))
  def judgeSingleLogic(labelList: ListBuffer[String], crowdSrc: String): Boolean = {

    val logic: String = getLogicType(crowdSrc)
    val subRules: JSONArray = getSubRules(crowdSrc)

    if (logic != null && subRules.size() > 0) { // 如果是逻辑结构，调用此方法本身，判断每个subRule是否满足，并用与或关联
      if (logic == "AND") {
        // judgeSingleLogic(labelList, subRules.forEach) //all be true
        for (i <- 0 until subRules.size()) {
          val rules: String = subRules.get(i).toString
          //          if (judgeSingleLogic(labelList, rules == false)) return false
          if (!judgeSingleLogic(labelList, rules)) return false
        }
        true
      }
      else if (logic == "OR") {
        //judgeSingleLogic(labelList, subRule.foreach) // at least one be true
        for (i <- 0 until subRules.size()) {
          val rules: String = subRules.get(i).toString
          if (judgeSingleLogic(labelList, rules)) return true
        }
        false
      }
      else {
        false
      }
    } else { // 如果是规则结构，判断labelList中是否包含此结构
      if (labelList.contains(getLabelId(crowdSrc))) true else false
    }
  }


}
