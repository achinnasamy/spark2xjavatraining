package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dharshekthvel on 19/12/17.
 */
public class ParallalizeAndMakeRDD {

    public static void main(String args[]) {

        SparkConf sparkConfig = new SparkConf()
                .setAppName("SparkReadJOB")
                .setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);


        List<String> dataList = new ArrayList<String>();
        dataList.add("Cassandra");
        dataList.add("MongoDB");
        dataList.add("Voldemort");

        JavaRDD<String> dataRDD = javaSparkContext.parallelize(dataList);

        dataRDD.collect().forEach(each -> System.out.println(each));

    }
}
