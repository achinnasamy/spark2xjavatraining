package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 * Created by dharshekthvel on 19/12/17.
 */
public class DiffBetweenPersistAndCheckpoint {

    public static void main(String args[]) {

        SparkConf sparkConfig = new SparkConf()
                                        .setAppName("UNDataRead")
                                        .setMaster("local[8]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
        javaSparkContext.setCheckpointDir("/home/dharshekthvel/ac/code/scalatrainingintellij/data/cp");

        JavaRDD<String> rdd = javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv");


        JavaRDD<String> dataSetRDD = rdd.sample(false,0.01);

        JavaRDD<VehicleDataDTOBean> dataBeanRDD = dataSetRDD.map((each) -> {

            String[] splitColumns = each.split(",");

            VehicleDataDTOBean dto = new VehicleDataDTOBean();
            dto.setEventNumber(splitColumns[0]);
            dto.setOpdDate(splitColumns[3]);
            dto.setVehicleID(splitColumns[4]);
            dto.setStopPosition(splitColumns[13]);
            dto.setPlanStatus(splitColumns[25]);

            return dto;
        });



        JavaPairRDD<String, Integer> pairRDD = dataBeanRDD.mapToPair(each -> new Tuple2<String, Integer>(each.getEventNumber(), Integer.parseInt(each.getStopPosition())));


        JavaPairRDD<String, Integer> countOfTripStops = pairRDD.reduceByKey((x,y) -> x+y);

        //countOfTripStops.collect().forEach(each -> System.out.println(each._1() + " --- " + each._2()));


        /**
         With persist() or cache(), the lineage is maintained. So data can be recomputed, if some partitions are lost.
         With checkpoint(), the lineage is not maintained
         */
        countOfTripStops.checkpoint();
        //countOfTripStops.cache();


        countOfTripStops.collect();
        /**
            With persist(), the lineage is maintained.
            With checkpoint(), the lineage is not maintained
         */
        System.out.println(countOfTripStops.toDebugString());





    }

}


