package scala.core

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{DoubleAccumulator, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 累加器
  * Created by zhao on 2017/8/16.
  */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("AccumulatorDemo").master("local").getOrCreate()
    val fileRdd: RDD[String] = ss.sparkContext.textFile("E:\\project\\SparkOperator\\src\\main\\resources/name.txt")
    val accumulator: LongAccumulator = ss.sparkContext.longAccumulator
    val count: Long = fileRdd.flatMap(_.split(" ")).map( x => accumulator.add(1)).count()
    println(count)
    println(accumulator.value)
  }
}
