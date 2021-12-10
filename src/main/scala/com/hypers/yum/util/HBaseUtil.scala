package com.hypers.yum.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object HBaseUtil {


  val conf: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
  val conn: Connection = ConnectionFactory.createConnection(HBaseConfiguration.create(conf))

  def getConn(): Connection = conn // 长链接，规避多次构造连接消耗，若有性能问题可改写为线程池


}
