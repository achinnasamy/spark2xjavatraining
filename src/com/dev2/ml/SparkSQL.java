package com.dev2.ml;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by dharshekthvel on 18/12/17.
 */
public class SparkSQL {

    public static void main(String[] args) {



        SparkSession ss = SparkSession
                                    .builder()
                                    .appName("SparkSqlJOB")
                                    .master("local[*]")
                                    .getOrCreate();


        Dataset<Row> df = ss.sqlContext().read().option("header","true")
                                    .csv("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv");


        df.show();

    }
}
