package com.naili.trans

import org.apache.spark.rdd.RDD

class Search(query:String) {

  def isMatch(s: String): Boolean = {
    s.contains(query)
  }

  def getMatch1(rdd: RDD[String]): RDD[String] = {
    rdd.filter(this.isMatch)
  }

  def getMatch2(rdd: RDD[String]): RDD[String] = {
    rdd.filter(x => x.contains(query))
  }

  def getMatch3(rdd: RDD[String]): RDD[String] = {
    val str = this.query
    rdd.filter(x => x.contains(str))
  }
}