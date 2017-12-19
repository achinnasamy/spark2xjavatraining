package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

public class SparkMySQL {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession
				.builder()
				.appName("Java Spark SQL basic example")
				.master("local[*]")
				.getOrCreate();


		Dataset<Row> jdbcDF = sparkSession.read()
					.format("jdbc")
					.option("url", "jdbc:mysql://127.0.0.1:3306/slz_core")
					.option("user", "slz02")
					.option("password", "slz02@123")
					.option("dbtable", "regions")
					.load();


		jdbcDF.rdd();



	}

}
