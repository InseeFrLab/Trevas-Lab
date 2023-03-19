package fr.insee.trevas.lab;

import fr.insee.vtl.spark.SparkDataset;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
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

    @Disabled
    @Test
    void sasLoading() throws ScriptException {
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master("local");
        SparkSession spark = sparkBuilder.getOrCreate();

        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        ScriptContext context = engine.getContext();
        Bindings bindings = new SimpleBindings();
        Dataset<Row> sas = spark.read()
                .format("com.github.saurfang.sas.spark")
                .load("src/main/resources/sas/sample.sas7bdat");
        bindings.put("sas", new SparkDataset(sas));
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        engine.put("$vtl.engine.processing_engine_names", "spark");
        engine.put("$vtl.spark.session", spark);

        engine.eval("sas_out := sas;");

        engine.getContext().getAttribute("sas_out");
    }

}
