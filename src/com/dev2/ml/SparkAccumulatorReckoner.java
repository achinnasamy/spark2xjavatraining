package com.dev2.ml;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.LongAccumulator;
import scala.Function1;

public class SparkAccumulatorReckoner {

	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
										.setAppName("AccumulatorJOB")
										.setMaster("local[8]");


		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// For Java Spark Context,
		// Fetching the accumulator is from
		LongAccumulator planStatusAccumulator = javaSparkContext.sc().longAccumulator();
		LongAccumulator nonPlanStatusAccumulator = javaSparkContext.sc().longAccumulator();

		long totalCount = javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv", 10)
						    .map(each -> {

								String[] splittedInput = each.split(",");

								if (splittedInput[25].equals("P"))
									planStatusAccumulator.add(new Integer(1));
								else
									nonPlanStatusAccumulator.add(new Integer(1));


						    	return each;
							}).count();

		System.out.println("Plan status count - " + planStatusAccumulator.value());

		System.out.println("Non status count - " + nonPlanStatusAccumulator.value());
		System.out.println("Total Count - " + totalCount);

		javaSparkContext.stop();
	}
}

