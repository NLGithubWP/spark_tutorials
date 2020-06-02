package com.naili.accu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestCustomerAccu {
  def main(args: Array[String]):Unit = {

    // create spark conf

    val sparkConf = new SparkConf().setAppName("TestCusAccu").setMaster("local[*]")

    // create spark context
    val sc = new SparkContext(sparkConf)

    //create
    var accu = new CustomerAccu

    //register
    sc.register(accu, "sum")

    //rdd
    val value: RDD[Int] = sc.parallelize(Array(1,2,3,4))

    val maped:RDD[Int] = value.map(x=>{
      accu.add(x)
      x
    })

    maped.collect().foreach(println)

    println("*****")

    println(accu.value)

    sc.stop()

  }
}
