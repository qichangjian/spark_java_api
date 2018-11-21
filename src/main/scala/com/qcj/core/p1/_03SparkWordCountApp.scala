package com.qcj.core.p1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  * 把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  * Read读
  * Eva求值
  * Print打印
  * Loop循环 再来一遍
  * repl 交互式查询
  *
  */
object _03SparkWordCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(s"${_03SparkWordCountApp.getClass.getSimpleName}")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val linesRDD = sc.textFile("hdfs://bd1807/1.txt")
    val wordsRDD = linesRDD.flatMap(line => line.split("\\s+"))
    val pairsRDD = wordsRDD.map(word => (word,1))
    val rbkRDD = pairsRDD.reduceByKey((v1,v2)=>v1+v2)
    rbkRDD.foreach(println)

    sc.stop()
  }
}
