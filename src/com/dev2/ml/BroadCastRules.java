package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * Created by dharshekthvel on 18/12/17.
 */
public class BroadCastRules {

    public static void main(String[] args) {
        SparkConf sparkConfig = new SparkConf()
                                        .setAppName("UNDataRead")
                                        .setMaster("local[8]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

        VehicleDataDTOBean bean = new VehicleDataDTOBean();
        bean.setPlanStatus("U");

        Broadcast<VehicleDataDTOBean> bc = javaSparkContext.broadcast(bean);


        javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/auth.csv")
                        .map(each -> {
                            System.out.println(bc.value().getPlanStatus());
                            return each;})
                        .collect();
    }

}
