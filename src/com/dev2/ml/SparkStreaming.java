package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreaming {

	public static void main(String args[]) {


		try {
			SparkConf sparkConfig = new SparkConf()
					.setAppName("SparkStreaming")
					.setMaster("local[5]");

			JavaStreamingContext jsc = new JavaStreamingContext(sparkConfig, Durations.seconds(10));

			JavaReceiverInputDStream<String> streamOfLines = jsc.socketTextStream("localhost", 5555);


			streamOfLines.print();

			jsc.start();
			jsc.awaitTermination();
		}
		catch (InterruptedException e) {

		}
	}
}
