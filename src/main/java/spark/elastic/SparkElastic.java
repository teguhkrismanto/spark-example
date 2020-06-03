package spark.elastic;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.fasterxml.jackson.core.JsonProcessingException;

public class SparkElastic {
	public static void main(String[] args) throws JsonProcessingException {		
		SparkConf conf = new SparkConf().set("es.nodes", "http://192.168.180.215:9200/").set("es.port", "9200")
				.set("es.resource", "ima-printed-news-hot-*");

		SparkSession sparkSession = SparkSession.builder().master("local[2]").config(conf).getOrCreate();

		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

		JavaPairRDD<String,Map<String,Object>> esRDD = JavaEsSpark.esRDD(sparkContext);
		JavaRDD<Map<String, Object>> rdd = JavaEsSpark.esRDD(sparkContext).values();

		System.out.println(rdd.count());
	}
}
