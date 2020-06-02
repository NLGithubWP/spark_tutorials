package com.naili.test.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object KafkaStreaming {


  // 获取上一次消费的位置
  def getOffset(kafkaCluster: KafkaCluster, group: String, topic: String): Map[TopicAndPartition, Long] = {

    // 定义一个存放主题分区offset 的map
    val topicAndPartitionToLong: mutable.Map[TopicAndPartition, Long] = new mutable.HashMap[TopicAndPartition, Long]()

    // 获取分区信息
    val partitions: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //判断是否有分区信息
    if(partitions.isRight) {

      //取出分区信息
      val topicAndPartitions: Set[TopicAndPartition] = partitions.right.get

      //get offset
      val topicAndPartitionToOffset: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitions)

      // 判断是否是已经保存了offset
      if(topicAndPartitionToOffset.isLeft){

        //没有消费过
        for (topicAndPartition <- topicAndPartitions){
          topicAndPartitionToLong += (topicAndPartition -> 0L)
        }
      }
      else{
        //已经消费过了
        val offsets: Map[TopicAndPartition, Long] = topicAndPartitionToOffset.right.get

        for (offset <- offsets){
          topicAndPartitionToLong += offset
        }
      }
    }

    topicAndPartitionToLong.toMap
  }

  def setOffsets(kafkaCluster: KafkaCluster, kafkaStream: InputDStream[String], group: String): Unit = {

    kafkaStream.foreachRDD(rdd => {

      // 从rdd获取 offset
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      // 遍历offset Range
      for (offsetRange <- offsetRanges) {
        val offset: Long = offsetRange.fromOffset

        //保存offset
        val ack: Either[Err, Map[TopicAndPartition, Short]] =
          kafkaCluster.setConsumerOffsets(group, Map(offsetRange.topicAndPartition() -> offset))

        if (ack.isLeft){
          println(s"Error: ${ack.left.get}")
        }
      }
    })
  }

  def main(args: Array[String]): Unit = {

    // create SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreaming")

    // create StreamContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    // kafka 参数定义
    val brokers = "localhost:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"

    //kafka 参数
    val kafkaPara: Map[String, String] = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG->brokers,
      ConsumerConfig.GROUP_ID_CONFIG->group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG->deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG->deserialization
    )

    //获取kafka Cluster 对象
    val kafkaCluster: KafkaCluster = new KafkaCluster(kafkaPara)

    //读取上一次消费到数据位置的
    val fromOffset: Map[TopicAndPartition, Long] = getOffset(kafkaCluster, group, topic)

    // 读取kafka数据
    val kafkaStream: InputDStream[String] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
        ssc, kafkaPara, fromOffset, (message: MessageAndMetadata[String, String]) => message.message())

    //保存
    kafkaStream.map((_,1)).reduceByKey(_+_).print()

    //保存offset
    setOffsets(kafkaCluster, kafkaStream, group)

    //开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
