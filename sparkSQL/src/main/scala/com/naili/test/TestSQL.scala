package com.naili.test

import org.apache.spark.sql.{DataFrame, DataFrameNaFunctions, SparkSession}

object TestSQL {

  def main(args: Array[String]):Unit = {

    val spark:SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("TestSQL")
      .getOrCreate()

    // import implicite transfer

    import spark.implicits._

    val filepath = "/Users/nailixing/big_data_scripts/spark-2.4.5-bin-hadoop2.7/examples/src/main/resources/people.json"

    val dataFrame: DataFrame = spark.read.json(filepath)

    //DSL style
    dataFrame.filter($"age">20).show()

    //SQL style
    dataFrame.createTempView("people")

    spark.sql("select * from people").show()

    spark.stop()

  }
}
