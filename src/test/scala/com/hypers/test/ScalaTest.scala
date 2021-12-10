package com.hypers.test

import com.hypers.yum.util.HBaseUtil
import org.junit.Test

class ScalaTest {


  @Test
  def test1:Unit = {

    println(HBaseUtil.getConn())
    println(HBaseUtil.getConn())
    println(HBaseUtil.getConn())

  }








}
