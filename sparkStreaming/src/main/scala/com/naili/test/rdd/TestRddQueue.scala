package com.naili.test.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object TestRddQueue {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("TestRDDQueue")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //创建rdd队列
    val SeqToRdds = new mutable.Queue[RDD[Int]]

    //创建rdd队列, 创建dstream
    val rddDStream = ssc.queueStream(SeqToRdds)

    //累加结果
    rddDStream.reduce(_+_).print

    ssc.start()

    for (i <- 1 to 5){

      SeqToRdds += ssc.sparkContext.makeRDD(1 to 100, 10)
      Thread.sleep(1000)
    }

    ssc.awaitTermination()

  }
}
