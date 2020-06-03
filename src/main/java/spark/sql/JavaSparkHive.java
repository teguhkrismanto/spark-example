package spark.sql;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSparkHive {

	public static class Record implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		private int key;
		private String value;

		public int getKey() {
			return key;
		}

		public void setKey(int key) {
			this.key = key;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Java Spark Hive Example").master("local[2]")
				.enableHiveSupport().getOrCreate();

		spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
		spark.sql("LOAD DATA LOCAL INPATH '/home/ebdesk/i2-workspace/spark/src/main/resources/kv1.txt' INTO TABLE src");

		spark.sql("SELECT COUNT (*) FROM src").show();

		Dataset<Row> sqlDF = spark.sql("SELECT key, value FROM src WHERE key > 10 ORDER BY key DESC");
		Dataset<String> stringsDS = sqlDF.map(
				(MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1), Encoders.STRING());
		stringsDS.show();

		List<Record> records = new ArrayList<>();
		for (int key = 1; key < 100; key++) {
			Record record = new Record();
			record.setKey(key);
			record.setValue("val_" + key);
			records.add(record);
		}
		Dataset<Row> recordsDF = spark.createDataFrame(records, Record.class);
		recordsDF.createOrReplaceTempView("records");

//		spark.sql("SELECT * FROM records r JOIN src ON r.key = s.key").show();

		spark.stop();
	}
}
