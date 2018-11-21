package com.qcj.core.p2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 主要学习SparkTransformation的操作算子
  * 1、map：将集合中每个元素乘以7
  * 2、filter：过滤出集合中的奇数
  * 3、flatMap：将行拆分为单词
  * 4、sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
  * 5、union：返回一个新的数据集，由原数据集和参数联合而成
  * 6、groupByKey：对数组进行 group by key操作 慎用
  * 7、reduceByKey：统计每个班级的人数
  * 8、join：打印关联的组合信息
  * 9、sortByKey：将学生身高进行排序
  * 10、combineByKey
  * 11、aggregateByKey
  */
object _01SparkTransformationOps {



  def main(args: Array[String]): Unit = {
    //设置日志打印级别log
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[2]").setAppName(s"${_01SparkTransformationOps.getClass.getSimpleName}")
    val sc = new SparkContext(conf)
    val list = List(1,2,3,4,5,6,7)

//    transformationMap_01(list,sc)
//    transformationFlatMap_02(sc)
//    transformationFilter_03(list,sc)
//    transformationSample_04(sc)
    transformationUnion_05(sc)
//    transformationGBK_06(sc)
//    transformationRBK_07(sc)
//    transformationJOIN_08(sc)
//    transformationsbk_09(sc)
    sc.stop()
  }

  /**
    * 1、map：将集合中每个元素乘以7
    *   map是最常用的转换算子之一，将原rdd的形态，转化为另外一种形态，
    *   需要注意的是这种转换是one-to-one
    */
  def transformationMap_01(list: List[Int], sc: SparkContext) = {
    val listRDD = sc.parallelize(list)
    val retRDD = listRDD.map(num => num * 7)
    retRDD.foreach(println)
  }

  /**
    * 3、flatMap：将行拆分为单词
    *     和map算子类似，只不过呢，rdd形态转化对应为one-to-many
    * @param sc
    */
  def transformationFlatMap_02(sc:SparkContext): Unit = {
    val list = List("lu jia hui","chen zhi xing")
    val listRDD = sc.parallelize(list)
    listRDD.flatMap(line=>line.split("\\s+")).foreach(println)
  }

  /**
    * filter：过滤出集合中的奇数(even)
    */
  def transformationFilter_03(list:List[Int],sc:SparkContext):Unit={
    val listRDD = sc.parallelize(list)
    val filterRDD = listRDD.filter(num => {
      //懒加载：如果只是编译时不报错的，当第一次使用的时候才会报错
//      1 / 0
      num % 2 == 0
    })
    filterRDD.foreach(println)
  }

  /**
    * sample：根据给定的随机种子seed，随机抽样出数量为frac的数据
    * 参数：
    *     withReplacement：true或者false
    *         true：代表有放回的抽样
    *         false：代表无放回的抽样
    *     fraction：抽取样本空间占总体的比例(分数的形式传入)
    *         without replacement： 0 <= fraction <= 1
    *         with replacement: fraction >= 0
    *     seed:随机数生成器
    *     new Random().nextInt(10)
    *     注意：我们使用sample算子不能保证提供集合大小就恰巧是rdd.size * fraction,
    * 结果大小会在前面数字上下浮动
    *   sample算子，在我们后面学习spark调优(dataskew)的时候，可以用的到
    */
  def transformationSample_04(sc: SparkContext) = {
    val list = 0 to 9999
    val listRDD = sc.parallelize(list)
    val sampleRDD = listRDD.sample(false,0.05)
    println(s"sampleRDD的集合大小：${sampleRDD.count()}")
  }

  /**
    * union：返回一个新的数据集，由原数据集和参数联合而成
    *   该union操作和sql中的union all操作一模一样
    */
  def transformationUnion_05(sc: SparkContext) = {
    val list1 = List(1,2,3,4,5)
    val list2 = List(3,4,5,6,7)
    val LRDD1 = sc.parallelize(list1)
    val LRDD2 = sc.parallelize(list2)
    LRDD1.union(LRDD2).foreach(t=>println(t+" "))
  }

  /**
    * groupByKey：对数组进行 group by key操作 慎用
    * 一张表，student表
    *     stu_id  stu_name   class_id
    * 将学生按照班级进行分组，把每一个班级的学生整合到一起
    * 建议groupBykey在实践开发过程中，能不用就不用，主要是因为groupByKey的效率低，
    * 因为有大量的数据在网络中传输，而且还没有进行本地的预处理
    * 我可以使用reduceByKey或者aggregateByKey或者combineByKey去代替这个groupByKey
    */
  def transformationGBK_06(sc: SparkContext) = {
    val list = List(
      "1  郑祥楷 1807bd-bj",
      "2  王佳豪 1807bd-bj",
      "3  刘鹰 1807bd-sz",
      "4  宋志华 1807bd-wh",
      "5  刘帆 1807bd-xa",
      "6  何昱 1807bd-xa"
    )
    val listRDD = sc.parallelize(list)
    println("分区个数：" + listRDD.getNumPartitions)

    val gbkInfo = listRDD.map { case (line) => {
      val fields = line.split("\\s+")
      (fields(2), 1)
    }}
    val gbkRDD = gbkInfo.groupByKey()
    gbkRDD.foreach{case (cid,stus)=>{
      println(s"${cid}--->${stus}--->${stus.size}")
    }}
  }

  /**
    *
    * 一张表，student表
    *     stu_id  stu_name   class_id
    *  统计每个班级的人数
    *  相同的统计下，reduceByKey要比groupByKey效率高，因为在map操作完毕之后发到reducer之前
    *  需要先进行一次本地的预聚合，每一个mapper(对应的partition)执行一次预聚合
    * @param sc
    */
  def transformationRBK_07(sc: SparkContext) = ???

  def transformationJOIN_08(sc: SparkContext) = ???

  def transformationsbk_09(sc: SparkContext) = ???
}
