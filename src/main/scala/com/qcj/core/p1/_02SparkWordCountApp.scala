package com.qcj.core.p1

import org.apache.spark.sql.SparkSession

/**spark2.x之后：
*      需要构建SparkSession，使用sparksession来进行构建sparkcontext
*      这个sparksession管理了sparkcontext，sqlcontext等等的创建。
*/
/**
  * java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  * 把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  */
object _02SparkWordCountApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(s"${_02SparkWordCountApp.getClass.getSimpleName}")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    val linesRDD = sc.textFile("hdfs://bd1807/1.txt")
    linesRDD.flatMap(_.split("\\s+")).map((_,1)).reduceByKey(_+_).foreach(println)

    spark.stop()
  }
}
