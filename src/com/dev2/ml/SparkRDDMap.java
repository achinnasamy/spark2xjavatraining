package com.dev2.ml;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class SparkRDDMap {


	public static void main(String args[]) {

		SparkConf sparkConfig = new SparkConf()
						//.set("spark.local.dir", "/Users/apple")
						.setAppName("ReadCSVFile")
						.setMaster("local[8]");
						
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);

		// 5 is the number of partitions
		JavaRDD<String> rdd = javaSparkContext.textFile("/home/dharshekthvel/ac/code/scalatrainingintellij/data/vs1.csv");
		

		/**
		 * 1,2,3 all are same
		 * 
		 */
		// 1
		JavaRDD<VehicleDataDTOBean> mappedRDD = rdd.map(new Function<String, VehicleDataDTOBean>() {
									
														@Override
														public VehicleDataDTOBean call(String input) throws Exception {
															
															String[] splitColumns = input.split(",");

															VehicleDataDTOBean dto = new VehicleDataDTOBean();
															dto.setEventNumber(splitColumns[0]);
															dto.setOpdDate(splitColumns[3]);
															dto.setVehicleID(splitColumns[4]);
															dto.setStopPosition(splitColumns[13]);
															dto.setPlanStatus(splitColumns[25]);
															return dto;
														}
													});
		
//		// 2
		JavaRDD<VehicleDataDTOBean> vehicleDataBeanRDD = rdd.map(new VehicleDataBeanConverterFunction());
//
//		// 3
		JavaRDD<VehicleDataDTOBean> undataBeanRDD = rdd.map((each) -> {

				String[] splitColumns = each.split(",");

				VehicleDataDTOBean dto = new VehicleDataDTOBean();
				dto.setEventNumber(splitColumns[0]);
				dto.setOpdDate(splitColumns[3]);
				dto.setVehicleID(splitColumns[4]);
				dto.setStopPosition(splitColumns[13]);
				dto.setPlanStatus(splitColumns[25]);

				return dto;
		});
		
		mappedRDD.collect().forEach(each -> System.out.println(each.getPlanStatus()));

		//lcRDD.foreach(param -> System.out.println(param.getCountry()));

		//lcRDD.saveAsTextFile("/Users/apple/saver002");
		//lcRDD.saveAsObjectFile("/Users/apple/saverzz");
		
		//countryNamesRDD.saveAsTextFile("/Users/apple/countryNamesSaver");
		
		javaSparkContext.close();
		javaSparkContext.stop();
		
	}
}

class VehicleDataBeanConverterFunction implements Function<String, VehicleDataDTOBean> {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;


	@Override
	public VehicleDataDTOBean call(String input) throws Exception {

		String[] splitColumns = input.split(",");

		VehicleDataDTOBean dto = new VehicleDataDTOBean();
		dto.setEventNumber(splitColumns[0]);
		dto.setOpdDate(splitColumns[3]);
		dto.setVehicleID(splitColumns[4]);
		dto.setStopPosition(splitColumns[13]);
		dto.setPlanStatus(splitColumns[25]);
		return dto;
	}

}

class VehicleDataDTOBean implements Serializable {

	private String eventNumber 		= "";

	private String opdDate 			= "";

	private String vehicleID 		= "";

	private String stopPosition 	= "";

	private String planStatus 		= "";

	public String getEventNumber() {
		return eventNumber;
	}

	public void setEventNumber(String eventNumber) {
		this.eventNumber = eventNumber;
	}

	public String getOpdDate() {
		return opdDate;
	}

	public void setOpdDate(String opdDate) {
		this.opdDate = opdDate;
	}

	public String getVehicleID() {
		return vehicleID;
	}

	public void setVehicleID(String vehicleID) {
		this.vehicleID = vehicleID;
	}

	public String getStopPosition() {
		return stopPosition;
	}

	public void setStopPosition(String stopPosition) {
		this.stopPosition = stopPosition;
	}

	public String getPlanStatus() {
		return planStatus;
	}

	public void setPlanStatus(String planStatus) {
		this.planStatus = planStatus;
	}
}

