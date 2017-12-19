package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkReadFile {
	
	public static void main(String[] args) {
		
		
		SparkConf sparkConfig = new SparkConf()
						.setAppName("SparkReadJOB")
						.setMaster("local[*]");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		
		javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv")
						.foreach(eachLine -> System.out.println(eachLine));
		
		javaSparkContext.close();
		javaSparkContext.stop();
	
	}
}
