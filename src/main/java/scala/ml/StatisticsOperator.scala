package scala.ml

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by zhao on 2017/9/7.
  * 列统计汇总
  *
  */
object StatisticsOperator {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("StatisticsOperator").master("local").getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    //读取数据,转换成RDD[Vector]类型
    val fileRdd: RDD[String] = ss.sparkContext.textFile(StatisticsOperator.getClass.getClassLoader.getResource("sample_stat.txt").getPath)
    val doubleRdd: RDD[Array[Double]] = fileRdd.map(_.split("@%split!@")).map(_.map(_.toDouble))
    val vectorRdd: RDD[Vector] = doubleRdd.map(Vectors.dense(_))
    vectorRdd.foreach(println)
    ss.stop()

  }
}
