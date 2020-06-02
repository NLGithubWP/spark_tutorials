package com.naili.accu

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object TestAccu {
  def main(args: Array[String]):Unit = {

    // create spark conf

    val sparkConf = new SparkConf().setAppName("TestAccu").setMaster("local[*]")

    // create spark context
    val sc = new SparkContext(sparkConf)

    //
    var sum = 0

    val sum2: LongAccumulator = sc.longAccumulator("sum")

    //rdd
    val value: RDD[Int] = sc.parallelize(Array(1,2,3,4))

    val maped: RDD[Int] = value.map(x=>{
      sum += x
      sum2.add(x)
      x
    })

    maped.collect().foreach(println)

    maped.foreach(println)

    //sum 还是0，因为没发送回来
    println(sum)

    //sum2 =10, acc return to driver， and driver will merge the res of 2 partitons
    println(sum2.value)

    sc.stop()

  }
}
