package spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Function1;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.AnalysisException;

public class JavaSparkSQL {

	public static class Person {
		private String name;
		private int age;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

	}

	public static void main(String[] args) throws AnalysisException {
		SparkSession sparkSession = SparkSession.builder().master("local[2]").getOrCreate();
		
//		runBasicData(sparkSession);
		runProgrammaticSchema(sparkSession);
	}

	private static void runBasicData(SparkSession sparkSession) throws AnalysisException {
		Dataset<Row> df = sparkSession.read().json("/home/ebdesk/i2-workspace/spark/src/main/resources/people.json");
		df.show();
		df.printSchema();
		df.select("name").show();
		df.select(col("name"), col("age").plus(1)).show();
		df.filter(col("age").gt(21)).show();
		df.groupBy("age").count().show();
		df.createOrReplaceTempView("people");

		Dataset<Row> sqlDF = sparkSession.sql("SELECT * FROM people");
		sqlDF.show();
		df.createGlobalTempView("people");
		sparkSession.sql("SELECT * FROM global_temp.people").show();
		sparkSession.newSession().sql("SELECT * FROM global_temp.people").show();
	}

	private static void runProgrammaticSchema(SparkSession spark) {
		ObjectMapper mapper = new ObjectMapper();
		
		JavaRDD<String> peopleRdd = spark.sparkContext().textFile("/home/ebdesk/i2-workspace/spark/src/main/resources/people.txt", 1)
				.toJavaRDD();
		String schemaString = "name age";

		List<StructField> fields = new ArrayList<>();
		for (String fieldName : schemaString.split(" ")) {
			StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
			fields.add(field);
		}

		StructType schema = DataTypes.createStructType(fields);

		JavaRDD<Row> rowRdd = peopleRdd.map(new Function<String, Row>() {

			@Override
			public Row call(String v1) throws Exception {
				String[] attributes = v1.split(",");
				return RowFactory.create(attributes[0], attributes[1].trim());
			}
		});

//		rowRdd.foreach(new VoidFunction<Row>() {
//
//			@Override
//			public void call(Row t) throws Exception {
//				System.out.println(mapper.writeValueAsString(t));
//				
//			}
//		});
		
		
		Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRdd, schema);
		peopleDataFrame.createOrReplaceTempView("people");

		Dataset<Row> results = spark.sql("SELECT * FROM people");
		Dataset<String> namesDs = results.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0) + ", Age: " + row.getString(1),
				Encoders.STRING());
		namesDs.show();
	}

}
