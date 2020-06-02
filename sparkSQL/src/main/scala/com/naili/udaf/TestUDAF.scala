package com.naili.udaf

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUDAF {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("TestSQL")
      .getOrCreate()

    // import implicite transfer

    import spark.implicits._

    val filepath = "/Users/nailixing/big_data_scripts/spark-2.4.5-bin-hadoop2.7/examples/src/main/resources/people.json"

    val dataFrame: DataFrame = spark.read.json(filepath)

    dataFrame.createTempView("people")

    //注册udaf
    spark.udf.register("myAvg", CustomerUDAF)

    // 使用udaf
    spark.sql("select myAvg(age) from people").show()

    // 关闭连接
    spark.stop()

  }
}
