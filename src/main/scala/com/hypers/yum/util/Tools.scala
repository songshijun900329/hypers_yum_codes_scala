package com.hypers.yum.util

import java.security.MessageDigest

/**
 * @Author 4
 * @Description //TODO some tools
 * @Date 2021/12/10
 **/
object Tools {

  /*
   * @Date 2021/12/10
   * @Description //TODO 根据输入文本生成其MD5加密hash字符串
   **/
  def makeMD5str(text:String):String = {

    // 指定MD5加密算法
    val digest: MessageDigest = MessageDigest.getInstance("MD5")

    // 对输入数据进行加密,过程是先将字符串中转换成byte数组,然后进行随机哈希
    val encoded:Array[Byte] = digest.digest(text.getBytes)

    // 将加密后的每个字节转化成十六进制，一个字节8位，相当于2个16进制，不足2位的前面补0
    val md5hash1 = encoded.map("%02x".format(_)).mkString

    md5hash1
  }


}
