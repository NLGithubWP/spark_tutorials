package com.naili.partitioner

import org.apache.spark.Partitioner

class CustomerPartitioner(partitions:Int) extends Partitioner{

  // get num of partitions
  override def numPartitions: Int = partitions

  // get the id of partition
  // spark 不能按照value分区
  override def getPartition(key: Any):Int = 0

}
