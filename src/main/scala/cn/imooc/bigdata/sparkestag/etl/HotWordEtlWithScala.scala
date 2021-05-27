package cn.imooc.bigdata.sparkestag.etl

import org.apache.spark.{SparkConf, SparkContext}

object HotWordEtlWithScala {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[1]")
      .setAppName("hot word scala")
    val sc = new SparkContext(config)
    val lines = sc.textFile("hdfs://namenode:8020/SogouQ.sample.txt")
    val total = lines.count()
    val hitCount = lines.map(x => x.split("\t")(3))
      .map(word => word.split(" ")(0).equals(word.split(" ")(1)))
      .filter(x => x == true).count()
    println("hit rate:" + (hitCount.toFloat / total.toFloat))
  }

}
