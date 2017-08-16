package scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhao on 2017/8/16.
  */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("AccumulatorDemo")
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile("src/name")

  }
}
