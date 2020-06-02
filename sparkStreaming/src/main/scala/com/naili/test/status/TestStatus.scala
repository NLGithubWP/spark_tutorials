package com.naili.test.status

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object TestStatus {

  def main(args: Array[String]): Unit = {

    // create spark conf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TestStream")

    // crate streamContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    // 设置checkpoint地址
    ssc.sparkContext.setCheckpointDir("./checkpointdir")

    //创建Dstream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    val wordAndOne: DStream[(String, Int)] = wordDStream.map((_,1))

    // define a update function
    val update = (values: Seq[Int], status: Option[Int]) => {

      //当前批次内容计算
      val sum: Int = values.sum

      // 取出上一次内容
      val lastStatus = status.getOrElse(0)

      Some(sum + lastStatus)
    }

    val wordAndCount: DStream[(String, Int)] = wordAndOne.updateStateByKey(update)

    wordAndCount.print

    //开启
    ssc.start()

    // 等待所有线程（receiver, driver等）推出
    ssc.awaitTermination()

  }
}
