package com.hypers.yum.test.mock

/**
 * @Author 4
 * @Description //TODO 模拟数据
 * @Date 2021/12/9
 * */
object MockObj {

  /*
  模拟：
  获取人群规则（HBASE表：swift:dim_crowd_src）数据的resultList
   */
  def getCrowdRuleList(): List[String] = {

    List(
      "{\"crowdCode\":\"crowdCode1\",\"crowdName\":\"人群1\",\"brand\":\"PH\",\"startTime\":1604160000001,\"startTimeFmt\":\"2020-11-01 00:00:00.000\",\"endTime\":1636473600000,\"endTimeFmt\":\"2021-11-10 00:00:00.000\",\"labelList\":[{\"labelId\":\"label-1\"},{\"labelId\":\"label-2\"}],\"crowdRule\":{\"logic\":\"and\",\"subRules\":[{\"logic\":null,\"subRules\":[],\"labelId\":\"label-1\"},{\"logic\":null,\"subRules\":[],\"labelId\":\"label-2\"}],\"labelId\":null},\"topic\":\"crowdTopic1\",\"activityStartTime\":\"2021-10-30 06:00:00.000\",\"activityEndTime\":\"2021-11-30 23:00:00.000\",\"createTime\":\"2021-10-21 17:00:21.000\",\"updateTime\":\"2021-10-21 17:00:21.000\"}",
      "{\"crowdCode\":\"crowdCode2\",\"crowdName\":\"人群2\",\"brand\":\"KFC\",\"startTime\":1604160000000,\"startTimeFmt\":\"2020-11-01 00:00:00.000\",\"endTime\":1636473600000,\"endTimeFmt\":\"2021-11-10 00:00:00.000\",\"labelList\":[{\"labelId\":\"label-1\"},{\"labelId\":\"label-2\"}],\"crowdRule\":{\"logic\":\"and\",\"subRules\":[{\"logic\":null,\"subRules\":[],\"labelId\":\"label-1\"},{\"logic\":null,\"subRules\":[],\"labelId\":\"label-2\"}],\"labelId\":null},\"topic\":\"crowdTopic2\",\"activityStartTime\":\"2021-10-30 06:00:00.000\",\"activityEndTime\":\"2021-11-30 23:00:00.000\",\"createTime\":\"2021-10-21 17:00:21.000\",\"updateTime\":\"2021-10-21 17:00:21.000\"}"
    )

  }

  /*
   * Description //TODO 模拟kafka中数据为'标签结果'数据
   **/
  def getLabelResult(): String = {
    val kafkaData = "{\"labelId\":\"label-001\",\"labelName\":\"首次购买早餐\",\"labelDesc\":\"首次购买早餐的描述\",\"labelType\":\"order\",\"brand\":\"PH\",\"startTime\":1604160000000,\"startTimeFmt\":\"2020-11-01 00:00:00\",\"endTime\":1636473600000,\"endTimeFmt\":\"2021-11-10 00:00:00\",\"updateTime\":1635696000000,\"updateTimeFmt\":\"2021-11-01 00:00:00\",\"updateFrequency\":\"daily\",\"matchInfo\":{\"labelMatchId\":\"572F88AE748BB1467453A9903954006E_label-001\",\"userCode\":\"123213e24a3\",\"mobile\":\"18818881888\",\"orderId\":\"1637659077353167020\",\"orderTime\":1604160000000,\"orderTimeFmt\":\"2020-11-01 00:00:00.000\",\"flowTime\":{\"orderTransTimeFmt\":\"2020-11-01 00:00:00.000\",\"orderTransSinkTimeFmt\":\"2020-11-01 00:00:00.000\",\"labelProcessTimeFmt\":\"2020-11-01 00:00:00.000\"}}}"
    kafkaData
  }


}
