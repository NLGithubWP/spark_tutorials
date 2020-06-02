package com.naili.trans

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestTrans {

  def main(args: Array[String]):Unit = {

    // create spark conf

    val sparkConf = new SparkConf().setAppName("TestTrans").setMaster("local[*]")

    // create spark context
    val sc = new SparkContext(sparkConf)

    val search = new Search("a")

    val words: RDD[String] = sc.parallelize(List("a", "b", "c", "d"))

    val filter: RDD[String] = search.getMatch3(words)

    filter.foreach(println)

    sc.stop()

  }
}
