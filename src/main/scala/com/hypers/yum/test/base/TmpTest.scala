package com.hypers.yum.test.base

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
    val aMap: mutable.Map[Int, Int] = mutable.Map[Int, Int]()
    aMap.foreach(println)
    aMap += (1 -> 11)
    aMap.foreach(println)
    aMap += (2 -> 22)

    println("after")

    aMap.foreach(println)


  }
}
