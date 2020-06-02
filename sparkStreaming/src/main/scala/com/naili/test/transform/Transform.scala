package com.naili.test.transform

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Transform {

  def main(args: Array[String]): Unit = {

    // create spark conf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestStream")

    // crate streamContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //创建Dstream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    // 转换成rdd
    val wordAndCountDstream: DStream[(String, Int)] = lineDStream.transform(rdd=> {

      val words: RDD[String] = rdd.flatMap(_.split(" "))

      val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

      val value: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

      value
    })

    wordAndCountDstream.print


    //开启
    ssc.start()

    // 等待所有线程（receiver, driver等）推出
    ssc.awaitTermination()
  }
}
