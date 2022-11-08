package fr.insee.trevas.lab;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@SpringBootTest
class TrevasLabApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	@Disabled
	void writeJson() {
		SparkSession.Builder sparkBuilder = SparkSession.builder()
				.appName("vtl-lab")
				.master("local");

		SparkSession spark = sparkBuilder.getOrCreate();

		String json = "{\"test\": \"ok\"}";

		JavaSparkContext.fromSparkContext(spark.sparkContext())
				.parallelize(List.of(json))
				.coalesce(1).rdd()
				.saveAsTextFile("src/main/resources/write.json");

		byte[] row = spark.read()
				.format("binaryFile")
				.load("src/main/resources/write.json")
				.first()
				.getAs("content");

		System.out.println(new String(row));
	}

}
