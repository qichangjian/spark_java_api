package com.qcj.core.p1

import org.apache.spark.{SparkConf, SparkContext}

/**
  * java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  * 把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  * scala中的s函数
  * https://blog.csdn.net/silviakafka/article/details/54576754
  *    //s函数的应用
  *    val name="Tom"
  *    s"Hello,$name"//Hello,Tom
  *    s"1+1=${1+1}"//1+1=2
  */
/**
  * 报错：
  * Error:scalac: bad option: '-make:transitive'
  * 解决方式：
  * 在项目.idea/下修改scala_compiler.xml文件，将此行parameter value="-make:transitive" />注释掉
  */
/**
  * java.lang.IllegalArgumentException: java.net.UnknownHostException: ns1
  * 把hadoop中的两个配置文件hdfs-site.xml和core-site.xml导入resources目录即可
  */
object _01SparkWordCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName(s"${_01SparkWordCountApp.getClass.getSimpleName}")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    val lineRDD = sc.textFile("hdfs://bd1807/1.txt")
    val wordsRDD = lineRDD.flatMap(line=>line.split("\\s+"))
    val pairsRDD = wordsRDD.map(word=>(word,1))
    val rbkRDD = pairsRDD.reduceByKey((v1,v2)=>v1+v2)
    rbkRDD.foreach(p=>println(p._1+"===="+p._2))
    sc.stop()
  }
}
