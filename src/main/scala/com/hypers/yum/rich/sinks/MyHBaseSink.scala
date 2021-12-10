package com.hypers.yum.rich.sinks

import cn.hutool.json.{JSONObject, JSONUtil}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.io.IOException

class MyHBaseSink(hTable_name:String) extends RichSinkFunction[String] {

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

    // Specify the column family name.
    val familyName = Bytes.toBytes("label")

    var hTable: Table = null
    try {

      println("value:\t" + value)

      if( !JSONUtil.isJson(value) ) return

      val jsonObject = JSONUtil.parseObj(value)
      val strLabelID = jsonObject.getStr("labelId")

      println("strLabelID:\t" + strLabelID)


      // Instantiate an HTable object.
      hTable = MyHBaseSink.conn.getTable(MyHBaseSink.hTableName)

//      val htable: HTable = new HTable(MyHBaseSink.conf, MyHBaseSink.hTableName)
//      htable.setAutoFlushTo(false)

      val puts: java.util.List[Put] = new java.util.ArrayList[Put](2)

      for ( index <- 0 to 1 ){ // just until 55 minute,drop after 55 minute



//        val strRowKey = MyHBaseSink.generateRowKey(inerJsonObj,strTimedList(index),strMeas_ID)
        val strRowKey = strLabelID

        // Instantiate a Put object.
        var put: Put = new Put(Bytes.toBytes(strRowKey)) // rowKey
        put.addColumn(familyName, Bytes.toBytes("jsonData"), Bytes.toBytes( value )) // family, qualifier, value
        System.out.println("put:\t"+put)

        puts.add(put)

      } // for's end





      // Submit a put request.
      hTable.put(puts)

//      htable.put(puts)
//      htable.flushCommits()
//      htable.close()


      System.out.println("After_HBase_Put_putValues:\t" + puts.toString);

    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (hTable != null) try // Close the HTable object.
        hTable.close()
      catch {
        case e: IOException => e.printStackTrace()
      }
    } // finally's end

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



  def generateRowKey( inerJsonObj: JSONObject,strMinute:String,strMeas_ID:String ):String = {


    null

  }
}













//    // Instantiating Configuration class
//    val conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
//    val conn = ConnectionFactory.createConnection(conf)
//
//
//
//    // Instantiating HTable class
//    val hTable:Table = conn.getTable(TableName.valueOf("test_htbl"))
//
//
//    // Instantiating Get class
//    val gdata = new Get(Bytes.toBytes("trw1"))
//
//    // Reading the data
//    val result = hTable.get(gdata)
//
//    // Reading values from Result class object
//    val value = result.getValue(Bytes.toBytes("labelId"),Bytes.toBytes("jsonData")) // family qualifier
//
//
//    // Printing the values
//    val strValue = Bytes.toString(value)
//
//    println("strHBASE-Value:\t" + strValue)
//
