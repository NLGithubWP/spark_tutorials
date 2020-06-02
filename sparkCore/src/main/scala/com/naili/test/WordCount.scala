package com.naili.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]):Unit = {

    // create spark conf

    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    // create spark context
    val sc = new SparkContext(sparkConf)

    // rad a file
    val line: RDD[String] = sc.textFile("/Users/nailixing/big_data_scripts/spark-2.4.5-bin-hadoop2.7/t.txt")

    print(line.partitions.length)

    val words: RDD[String] = line.flatMap(_.split(" "))

    val wordAndOne: RDD[(String, Int)] = words.map((_,1))

    val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    wordAndCount.saveAsTextFile("./res")


  }
}
