package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TPSpark {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> cities = context
				.textFile(args[0])
				.filter(line -> { 
					if(line.split(",")[4].isEmpty() ||
							line.split(",")[5].isEmpty() ||
							line.split(",")[6].isEmpty() ||
							line.split(",")[4].compareTo("Population")==0) 
						return false; 
					return true;
				})
				.map(res ->{
						String[] tmp=res.split(",");
						return tmp[4]+","+tmp[5]+","+tmp[6];
				});
		
		cities.saveAsTextFile(args[1]);
		context.close();
		
	}
	
}

