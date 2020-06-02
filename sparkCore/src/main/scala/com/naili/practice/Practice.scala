package com.naili.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Practice {

  def main(args: Array[String]) :Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Practice")

    val sc =new SparkContext(sparkConf)

    // read data as（ 时间，省份，城市， 用户 ，广告）
    val lines: RDD[String] = sc.textFile(" ")

    // 切割取出来省份和广告
    val proAndAdToOne: RDD[((String, String), Int)] = lines.map(x=> {
      val fields: Array[String] = x.split(" ")
      ((fields(1),fields(4)),1)
    })

    //统计省份广告被统计的总次数 (province, ad), 16
    val provinceAndAdToCount:RDD[((String, String), Int)] = proAndAdToOne.reduceByKey(_+_)

    //维度转换 （province (ad, 16)）
    val provinceToAdAndCount:RDD[(String, (String,Int ))] = provinceAndAdToCount.map(x=>(x._1._1, (x._1._2,x._2)))


    //聚合， （province, (ad1,16), (ad2,26), (ad3,46)）
    val provinceToAdGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdAndCount.groupByKey()

    val top3: RDD[(String, List[(String, Int)])] = provinceToAdGroup.mapValues(x=>
    {
      x.toList.sortWith((a,b) => a._2 > b._2).take(3)
    })

    top3.collect().foreach(println)

    // close sc
    sc.stop()








  }

}
