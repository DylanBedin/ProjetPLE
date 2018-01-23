package bigdata;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class TPSpark {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd = context.parallelize(Arrays.asList(1, 2, 3, 4),10);
		rdd = rdd.map((x) -> x*10);
		rdd = rdd.filter((x) -> x%2 == 0);
		System.out(rdd.count());
		rdd = rdd.filter((x) -> x > 3);
		System.out(rdd.count());
		
	}
	
}
