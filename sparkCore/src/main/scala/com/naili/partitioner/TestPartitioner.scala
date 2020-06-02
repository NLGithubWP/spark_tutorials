package com.naili.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestPartitioner {

  def main(args: Array[String]):Unit = {

    // create spark conf

    val sparkConf = new SparkConf().setAppName("TestPartitioner").setMaster("local[*]")

    // create spark context
    val sc = new SparkContext(sparkConf)

    val words: RDD[String] = sc.parallelize(List("a", "b", "c", "d"))

    // 编程kv-rdd
    val wordToOne: RDD[(String, Int)] = words.map((_,1))

    //check current partitions
    val valueIndex:RDD[(Int, (String, Int))] = wordToOne.mapPartitionsWithIndex((i,idm) => idm.map((i,_)))

    valueIndex.foreach(println)

    val repartitioned: RDD[(String, Int)] = wordToOne.partitionBy(new CustomerPartitioner(3))

    repartitioned.mapPartitionsWithIndex((i, idm) => idm.map((i,_))).foreach(println)

    sc.stop()

  }
}
