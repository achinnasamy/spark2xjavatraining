package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Created by dharshekthvel on 18/12/17.
 */
public class TransformationReckoner {

    public static void main(String args[]) {

        SparkConf sparkConfig = new SparkConf()
                                    .setAppName("TransformationJOB")
                                    .setMaster("local[8]");


        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
        JavaRDD<String> textFileRDD = javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv");


        /* WholeTextFile */
//        javaSparkContext.wholeTextFiles("/home/dharshekthvel/ac/code/scalatrainingintellij/data")
//                        .foreach(each -> System.out.println(each._2()));


        /* mapToPair and reduceByKey */
        /*
        JavaRDD<String> textFileRDD = javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv");

        JavaPairRDD<String, Integer> pairRDD = textFileRDD.mapToPair(each -> new Tuple2(each,1));

        JavaPairRDD<String, Integer> countRDD = pairRDD.reduceByKey((x,y) -> x+y);

        countRDD.foreach(x -> System.out.println(x));
        */

        /*
        JavaRDD<String> textFileRDD = javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv");

        JavaPairRDD<String, Integer> pairRDD = textFileRDD.mapToPair(x -> new Tuple2<String, Integer>(x,1));
        pairRDD.foreach(x -> System.out.println(x._2()));
        */


        /*
        textFileRDD.cartesian(textFileRDD);
        textFileRDD.union(textFileRDD);
        textFileRDD.intersection(textFileRDD);
        */

        textFileRDD.checkpoint();
        textFileRDD.isCheckpointed();


        textFileRDD.persist(StorageLevel.MEMORY_AND_DISK());

        // To print in the lineage
        textFileRDD.toDebugString();




    }

}
