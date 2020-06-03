package spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkBatch {
	public static void main(String[] args) throws JsonProcessingException {
		SparkSession sparkSession = SparkSession.builder().master("local[2]").getOrCreate();
		JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());

		ObjectMapper mapper = new ObjectMapper();

//		read file
		JavaRDD<String> rdd = sparkContext.textFile("/home/ebdesk/Downloads/covid-mei");

//		convert JavaRDD<String> to JavaRDD<JsonNode>
		JavaRDD<JsonNode> rdd2 = rdd.map(new Function<String, JsonNode>() {

			private static final long serialVersionUID = 1L;

			@Override
			public JsonNode call(String v1) throws Exception {
				return mapper.readValue(v1, JsonNode.class);
			}
		}).filter(new Function<JsonNode, Boolean>() {	/* filter some fields */

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(JsonNode v1) throws Exception {
				boolean tes = v1.get("jumlah_anggota_keluarga") != null;
				return tes;
			}
		}).filter(new Function<JsonNode, Boolean>() { /* filter some strings */

			private static final long serialVersionUID = 1L;

			@Override
			public Boolean call(JsonNode v1) throws Exception {
				boolean tes = v1.get("jumlah_anggota_keluarga").asText() != "null";
				return tes;
			}
		});

//		convert JavaRDD<JsonNode> to Model
		JavaRDD<Model> map = rdd2.map(new Function<JsonNode, Model>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Model call(JsonNode v1) throws Exception {
				Model model = new Model();
				model.setUser_name(v1.get("user_name").asText());
				model.setJumlah_anggota_keluarga(v1.get("jumlah_anggota_keluarga").asText());
				model.setNo_ponsel(v1.get("no_ponsel").asText());

				return model;
			}
		});

//		map.foreach(new VoidFunction<Model>() {
//
//			@Override
//			public void call(Model t) throws Exception {
//				System.out.println(mapper.writeValueAsString(t));
//			}
//		});

		System.out.println(map.count());

		sparkContext.close();
	}

}
