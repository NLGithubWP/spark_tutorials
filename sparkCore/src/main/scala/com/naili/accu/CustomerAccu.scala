package com.naili.accu

import org.apache.spark.util.AccumulatorV2

class CustomerAccu  extends AccumulatorV2[Int,Int]{

  var cusSum :Int = 0

  // 判断是否为空
  override def isZero: Boolean = cusSum == 0

  // 复制
  override def copy(): AccumulatorV2[Int, Int] = {
  val accu = new CustomerAccu
  accu.cusSum = this.cusSum
  accu
  }

  // 重制
  override def reset(): Unit = cusSum = 0

  // 添加值
  override def add(v: Int): Unit = cusSum += v

  // 合并executor端数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = this.cusSum += other.value

  // 返回值
  override def value: Int = this.cusSum

}
