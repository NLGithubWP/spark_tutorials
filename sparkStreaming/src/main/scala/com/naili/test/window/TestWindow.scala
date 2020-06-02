package com.naili.test.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object TestWindow {
  def main(args: Array[String]): Unit = {

    // create spark conf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestStream")

    // crate streamContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    //创建Dstream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_,1))

    // 窗口操作
    val wordAndCount: DStream[(String, Int)] = wordAndOne.reduceByKeyAndWindow((x:Int, y: Int ) => x+y, Seconds(6), Seconds(3))

    wordAndCount.print
    //开启
    ssc.start()

    // 等待所有线程（receiver, driver等）推出
    ssc.awaitTermination()
  }

}
