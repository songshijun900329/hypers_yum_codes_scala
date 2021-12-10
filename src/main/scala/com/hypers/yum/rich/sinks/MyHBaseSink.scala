package com.hypers.yum.rich.sinks

import cn.hutool.json.JSONUtil
import com.hypers.yum.util.HBaseUtil
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
 * @Author 4
 * @Description //TODO 自定义sink
 * @Date 2021/12/9
 * */
class MyHBaseSink(hTable_name: String) extends RichSinkFunction[String] {

  val HTABLE_NAME = hTable_name


  override def open(parameters: org.apache.flink.configuration.Configuration): Unit = {
    super.open(parameters)

    MyHBaseSink.conf = HBaseConfiguration.create
    //    MyHBaseSink.conf.addResource(new Path("file:///Users/morning/Documents/work/work_for_hypers/yum百胜/code/yum_code_scala/src/main/resources/hbase-site.xml")) // HDFS PATH FILE
    MyHBaseSink.hTableName = TableName.valueOf(HTABLE_NAME)
    MyHBaseSink.conn = ConnectionFactory.createConnection(MyHBaseSink.conf)
    //    MyHBaseSink.conn = ConnectionFactory.createConnection(HBaseConfiguration.create(MyHBaseSink.conf))

  }


  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    //    super.invoke(value, context)

    println("value:\t" + value)

    if (!JSONUtil.isJson(value)) return

    val jsonObject = JSONUtil.parseObj(value)
    val strLabelID = jsonObject.getStr("labelId")

    println("strLabelID:\t" + strLabelID)

    var valueMap: Map[Array[Byte], Array[Byte]] = Map()
    valueMap += (Bytes.toBytes("qualifier1") -> Bytes.toBytes("value1"))

    HBaseUtil.putHData(MyHBaseSink.conn, hTable_name, "label", strLabelID, valueMap)

  }


  override def close(): Unit = {

    if (MyHBaseSink.conn != null) try MyHBaseSink.conn.close()
    catch {
      case e: Exception => e.printStackTrace()
    }
    super.close()

  }


}

object MyHBaseSink {
  var hTableName: TableName = _
  var conf: org.apache.hadoop.conf.Configuration = _
  var conn: Connection = _
}













