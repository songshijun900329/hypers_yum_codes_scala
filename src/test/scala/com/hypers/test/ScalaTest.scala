package com.hypers.test

import com.hypers.yum.util.HBaseUtil
import org.junit.Test
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.security.MessageDigest


class ScalaTest {

  val LOG: Logger = LoggerFactory.getLogger(this.getClass.getName)

  private val CONF_DIR = System.getProperty("user.dir") + File.separator + "conf" + File.separator


  @Test
  def test1:Unit = {

    println(HBaseUtil.getConn())
    println(HBaseUtil.getConn())
    println(HBaseUtil.getConn())

  }


  @Test
  def testLog:Unit = {


    println(CONF_DIR)

    LOG.info("testlog")

  }



  @Test
  def testMD5f1():Unit = {

    // 指定MD5加密算法
    val digest = MessageDigest.getInstance("MD5")

    val text = "MD5 this text!"

    // 对输入数据进行加密,过程是先将字符串中转换成byte数组,然后进行随机哈希
    val encoded = digest.digest(text.getBytes)

    // 将加密后的每个字节转化成十六进制，一个字节8位，相当于2个16进制，不足2位的前面补0
    val md5hash1 = encoded.map("%02x".format(_)).mkString

    LOG.info("md5hash1 --> {}",md5hash1) // 53a679afd216477024fc22f7c929ca0c


  }


  @Test
  def testMD5f2():Unit = {

    val digest = MessageDigest.getInstance("MD5")

    val text = "MD5 this text!"

    val b = text.getBytes("UTF-8")

    digest.update(b,0,b.length)

    val md5hash2 = new java.math.BigInteger(1,digest.digest()).toString(16)

    LOG.info("md5hash2 --> {}",md5hash2) // 53a679afd216477024fc22f7c929ca0c

  }








}
