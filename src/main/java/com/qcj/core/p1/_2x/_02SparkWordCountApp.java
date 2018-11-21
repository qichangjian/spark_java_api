package com.qcj.core.p1._2x;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * spark2.x之前：
 *       需要手动new SparkContext
 *
 * 使用lambda表达式
 *
 * 这是本地方式运行，导入xml会报错
 */
public class _02SparkWordCountApp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName(_02SparkWordCountApp.class.getSimpleName()) ;
        conf.setMaster("local[2]");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> lineRDD = jsc.textFile("D:\\1.txt");

        JavaRDD<String> wordsRDD = lineRDD.flatMap(line -> {
            String[] fields = line.split("\\s+");
            return Arrays.asList(fields).iterator();
        });
        JavaPairRDD<String, Integer> pairsRDD = wordsRDD.mapToPair(word -> {
            return new Tuple2<String, Integer>(word, 1);
        });
        JavaPairRDD<String, Integer> rbkRDD = pairsRDD.reduceByKey((v1, v2) -> {
            return v1 + v2;
        });
        rbkRDD.foreach(pair->{
            System.out.println(pair._1+"==>"+pair._2);
        });

        jsc.close();
    }
}
