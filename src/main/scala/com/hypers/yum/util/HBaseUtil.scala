package com.hypers.yum.util

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Result, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import scala.collection.JavaConversions._

/**
 * @Author 4
 * @Description //TODO hbaseutil
 * @Date 2021/12/9
 * */
object HBaseUtil {


  val LOG: Logger = LoggerFactory.getLogger(this.getClass.getName)


  val conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
  val conn: Connection = ConnectionFactory.createConnection(HBaseConfiguration.create(conf))

  def getConn(): Connection = conn // 长链接，规避多次构造连接消耗，若有性能问题可改写为线程池


  /*
   * @Description //TODO 生成rowkey的函数
   **/
  def generateRowKey( strUserCode:String,strCrowdCode:String ):String = {

    if ( StringUtils.isNotBlank(strCrowdCode) ) {
      Tools.makeMD5str(strUserCode) + strCrowdCode
    } else {
      Tools.makeMD5str(strUserCode)
    }

  }


  /*
   * @Description //TODO 扫hbase全表
   * @Date 2021/12/9
   **/
  def getHTableScanList(hTable_name: String, hFamily: String, hQualifier: String): java.util.List[String] = {
    //    LOG.info("Entering getHTableScanList.")

    val valueList: java.util.List[String] = new java.util.ArrayList[String](500)

    var table: Table = null
    // Instantiate a ResultScanner object.
    var rScanner: ResultScanner = null

    try {

      // Create the hTable instance.
      table = conn.getTable(TableName.valueOf(hTable_name))

      // Instantiate a Scan object.
      val scan: Scan = new Scan
      scan.addColumn(Bytes.toBytes(hFamily), Bytes.toBytes(hQualifier))

      // Set the cache size.
      scan.setCaching(1000)

      // Submit a scan request.
      rScanner = table.getScanner(scan)

      // Print query results.
      var r: Result = rScanner.next
      while (r != null) {
        for (cell: Cell <- r.rawCells) {
          LOG.info("{}:{},{},{}",
            Bytes.toString(CellUtil.cloneRow(cell)),
            Bytes.toString(CellUtil.cloneFamily(cell)),
            Bytes.toString(CellUtil.cloneQualifier(cell)),
            Bytes.toString(CellUtil.cloneValue(cell))
          )
          valueList.add(Bytes.toString(CellUtil.cloneValue(cell)))
        } // for's end
        r = rScanner.next
      } // while's end


    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (rScanner != null) { // Close the scanner object.
        rScanner.close
      }
      if (table != null) try { // Close the HTable object.
        table.close
      } catch {
        case e: IOException =>
          LOG.error("Close table failed ", e.printStackTrace())
      }
    } // finally's end
    valueList
  }


  /*
   * @Description //TODO 根据rowkey获取value
   * @Date 2021/12/9
   * PS：
   * hbase若设置值存在版本为2，则一个rowkey对应2个cell
   */
  def getHDataByRowKey(hTable_name: String, hFamily: String, hQualifier: String, hRowKey: String): java.util.List[String] = {

    // hbase若设置值存在版本为2，则一个rowkey对应2个cell
    val valueList: java.util.List[String] = new java.util.ArrayList[String](2)

    // Specify the column family name.
    val familyName: Array[Byte] = Bytes.toBytes(hFamily)
    // Specify the column name.
    val qualifier: Array[Byte] = Bytes.toBytes(hQualifier)
    // Specify RowKey.
    val rowKey = Bytes.toBytes(hRowKey)

    var hTable: Table = null

    try {

      // Create the Configuration instance.
      hTable = conn.getTable(TableName.valueOf(hTable_name))

      // Instantiate a Get object.
      val get: Get = new Get(rowKey)

      // Set the column family name and column name.
      get.addColumn(familyName, qualifier)

      // Submit a get request.
      val result: Result = hTable.get(get)

      // Print query results.
      for (cell: Cell <- result.rawCells) {
        LOG.info(
          "{}:{},{},{}",
          Bytes.toString(CellUtil.cloneRow(cell)),
          Bytes.toString(CellUtil.cloneFamily(cell)),
          Bytes.toString(CellUtil.cloneQualifier(cell)),
          Bytes.toString(CellUtil.cloneValue(cell)))
        valueList.add(Bytes.toString(CellUtil.cloneValue(cell)))
      } // for's end

    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      if (hTable != null) try // Close the HTable object.
        hTable.close()
      catch {
        case e: IOException => LOG.error("Close table failed ", e.printStackTrace())
      }
    } // finally's end
    valueList
  }


  /*
   * @Date 2021/12/10
   * @Description //TODO put data to hbase
   **/
  def putHData():Unit = {

  }




} // object's end
