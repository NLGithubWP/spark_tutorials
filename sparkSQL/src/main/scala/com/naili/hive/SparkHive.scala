package com.naili.hive

import org.apache.spark.sql.SparkSession

object SparkHive {

  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("TestHive")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql("show tables").show()

    //close
    spark.stop()



  }

}
