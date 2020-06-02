package com.naili.test

import org.apache.arrow.vector.types.pojo.ArrowType.Struct
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import sun.jvm.hotspot.debugger.cdbg.IntType

object CreateDF {

  def main(args: Array[String]): Unit = {

    // create spark session

    val spark:SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateDF")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    // create df
    // 1. create rdd
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3,4,5))


    // 2. transfer to RDD[ROW]
    val rddrow: RDD[Row]=rdd.map(x=>Row(x))

    // 3. create structure type
    val structType = StructType(StructField("id", IntegerType) :: Nil )

    // 4. create data frame
    val df: DataFrame = spark.createDataFrame(rddrow, structType)

    df.show()

    df.write.json("./js")

    spark.stop

  }
}
