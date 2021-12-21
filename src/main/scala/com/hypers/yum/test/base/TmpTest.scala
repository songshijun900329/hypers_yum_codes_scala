package com.hypers.yum.test.base

import cn.hutool.json.{JSONObject, JSONUtil}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author wenl
 * @date 2021/12/16 16:53
 */
object TmpTest {
  def main(args: Array[String]): Unit = {
    /*    val valueList: ListBuffer[String] = ListBuffer[String]()
        valueList.append("1")
        valueList.append("2")
        valueList.append("3")

        println(valueList.length)
        valueList.foreach(println)*/

    /*
        val a: ListBuffer[Int] = ListBuffer(1, 2, 3)
        val b: ListBuffer[Int] = ListBuffer(4, 5, 6)
        a.foreach(print)

        a ++= b

        println("after:")
        a.foreach(print)
    */
    /*    val aMap: mutable.Map[Int, Int] = mutable.Map[Int, Int]()
        aMap.foreach(println)
        aMap += (1 -> 11)
        aMap.foreach(println)
        aMap += (2 -> 22)

        println("after")

        aMap.foreach(println)*/

    //    println("572F88AE748BB1467453A9903954006E_label-2".split("_")(0))


    //4）如果留下，则该人群结果取一部分key值+从当前kafka的标签结果的matchinfo中取一些值=拼成该人群规则对应的人群结果；
    // 来自hbase 通过逻辑计算的<人群规则> 的字段
    val crowdElem: String = "{\"crowdCode\":\"crowdCode1\",\"crowdName\":\"人群1\",\"brand\":\"PH\",\"startTime\":1604160000001,\"startTimeFmt\":\"2020-11-01 00:00:00.000\",\"endTime\":1636473600000,\"endTimeFmt\":\"2021-11-10 00:00:00.000\",\"labelList\":[{\"labelId\":\"label-1\"},{\"labelId\":\"label-2\"}],\"crowdRule\":{\"logic\":\"and\",\"subRules\":[{\"logic\":null,\"subRules\":[],\"labelId\":\"label-1\"},{\"logic\":null,\"subRules\":[],\"labelId\":\"label-2\"}],\"labelId\":null},\"topic\":\"crowdTopic1\",\"activityStartTime\":\"2021-10-30 06:00:00.000\",\"activityEndTime\":\"2021-11-30 23:00:00.000\",\"createTime\":\"2021-10-21 17:00:21.000\",\"updateTime\":\"2021-10-21 17:00:21.000\"}"
    val hbaseRowObj: JSONObject = JSONUtil.parseObj(crowdElem)
    val crowdCode: String = hbaseRowObj.getStr("crowdCode")
    val crowdName: String = hbaseRowObj.getStr("crowdName")
    val brand: String = hbaseRowObj.getStr("brand")
    val startTime: String = hbaseRowObj.getStr("startTime")
    val startTimeFmt: String = hbaseRowObj.getStr("startTimeFmt")
    val endTime: String = hbaseRowObj.getStr("endTime")
    val endTimeFmt: String = hbaseRowObj.getStr("endTimeFmt")
    //            val topic: JSONArray = hbaseRowObj.getJSONArray("topic")
    val labelList: String = hbaseRowObj.getStr("labelList")
    val topic: String = hbaseRowObj.getStr("topic")
    val activityStartTime: String = hbaseRowObj.getStr("activityStartTime")
    val activityEndTime: String = hbaseRowObj.getStr("activityEndTime")
    val createTime: String = hbaseRowObj.getStr("createTime")
    val updateTime: String = hbaseRowObj.getStr("updateTime")

    // 来自kafka 的<标签结果> 的字段
    val kafkaJsonObj: JSONObject = JSONUtil.parseObj("{\"labelId\":\"label-001\",\"labelName\":\"首次购买早餐\",\"labelDesc\":\"首次购买早餐的描述\",\"labelType\":\"order\",\"brand\":\"PH\",\"startTime\":1604160000000,\"startTimeFmt\":\"2020-11-01 00:00:00\",\"endTime\":1636473600000,\"endTimeFmt\":\"2021-11-10 00:00:00\",\"updateTime\":1635696000000,\"updateTimeFmt\":\"2021-11-01 00:00:00\",\"updateFrequency\":\"daily\",\"matchInfo\":{\"labelMatchId\":\"572F88AE748BB1467453A9903954006E_label-001\",\"userCode\":\"123213e24a3\",\"mobile\":\"18818881888\",\"orderId\":\"1637659077353167020\",\"orderTime\":1604160000000,\"orderTimeFmt\":\"2020-11-01 00:00:00.000\",\"flowTime\":{\"orderTransTimeFmt\":\"2020-11-01 00:00:00.000\",\"orderTransSinkTimeFmt\":\"2020-11-01 00:00:00.000\",\"labelProcessTimeFmt\":\"2020-11-01 00:00:00.000\"}}}")
    val matchInfoJsonObj: JSONObject = kafkaJsonObj.getJSONObject("matchInfo")
    val labelMatchId: String = matchInfoJsonObj.getStr("labelMatchId")
    val crowdMatchId: String = labelMatchId.split("_")(0) + "_" + crowdCode
    val userCode: String = matchInfoJsonObj.getStr("userCode")
    val mobile: String = matchInfoJsonObj.getStr("mobile")
    val orderId: String = matchInfoJsonObj.getStr("orderId")
    val orderTime: String = matchInfoJsonObj.getStr("orderTime")
    val orderTimeFmt: String = matchInfoJsonObj.getStr("orderTimeFmt")
    val flowTime: String = matchInfoJsonObj.getStr("flowTime")

    val matchInfobody: JSONObject = new JSONObject(true)
    matchInfobody.putOpt("labelMatchId", labelMatchId)
    matchInfobody.putOpt("crowdMatchId", crowdMatchId)
    matchInfobody.putOpt("userCode", userCode)
    matchInfobody.putOpt("mobile", mobile)
    matchInfobody.putOpt("orderId", orderId)
    matchInfobody.putOpt("orderTime", orderTime)
    matchInfobody.putOpt("orderTimeFmt", orderTimeFmt)
    matchInfobody.putOpt("flowTime", flowTime)


    // 转换为Json
    val crowdbody: JSONObject = new JSONObject(true)
    crowdbody.putOpt("crowdCode", crowdCode)
    crowdbody.putOpt("crowdName", crowdName)
    crowdbody.putOpt("brand", brand)
    crowdbody.putOpt("startTime", startTime)
    crowdbody.putOpt("startTimeFmt", startTimeFmt)
    crowdbody.putOpt("endTime", endTime)
    crowdbody.putOpt("endTimeFmt", endTimeFmt)
    crowdbody.putOpt("labelList", labelList)
    crowdbody.putOpt("topic", topic)
    crowdbody.putOpt("activityStartTime", activityStartTime)
    crowdbody.putOpt("activityEndTime", activityEndTime)
    crowdbody.putOpt("createTime", createTime)
    crowdbody.putOpt("updateTime", updateTime)
    crowdbody.putOpt("matchInfo", matchInfobody)


    println(crowdbody.toString)

  }
}
