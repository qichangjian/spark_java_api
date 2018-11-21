package com.qcj.core.p1._2x;

import org.apache.spark.sql.SparkSession;

/**
 * spark2.x之后：
 *      需要构建SparkSession，使用sparksession来进行构建sparkcontext
 *      这个sparksession管理了sparkcontext，sqlcontext等等的创建。
 *
 */
public class _01SparkWordCountApp_session {
    public static void main(String[] args) {
        SparkSession.builder().appName(_01SparkWordCountApp_session.class.getSimpleName()).master("local").getOrCreate();
    }
}
