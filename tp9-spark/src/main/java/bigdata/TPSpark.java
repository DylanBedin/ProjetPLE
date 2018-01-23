package bigdata;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TPSpark {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> cities = context.textFile(args[1]);
		JavaRDD<Integer> pop = cities.map(line ->{
			String[] tmp=line.split(",");
			return Integer.parseInt(tmp[4]);
		});
		pop.saveAsTextFile(args[2]);
		
		
	}
	
}
