package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class FirstAction {

	public static void main(String[] args) {
		SparkConf sparkConfig = new SparkConf()
				.setAppName("UNDataRead")
				.setMaster("local[8]");

		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		JavaRDD<String> rdd = javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/auth.csv");
		
		
		String firstRow = rdd.first();
		System.out.println(firstRow);
		
		
		
		List<String> firstOneRow = rdd.take(1);
		firstOneRow.forEach(z -> System.out.println(z));
		
		javaSparkContext.close();

	}

}
