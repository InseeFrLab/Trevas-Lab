package fr.insee.vtl.lab.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.model.S3ForBindings;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.script.*;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class Utils {

    private static final Logger logger = LogManager.getLogger(Utils.class);

    public static ScriptEngine initEngine(Bindings bindings) {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        ScriptContext context = engine.getContext();
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        return engine;
    }

    public static ScriptEngine initEngineWithSpark(Bindings bindings, SparkSession spark) {
        ScriptEngine engine = new ScriptEngineManager().getEngineByName("vtl");
        ScriptContext context = engine.getContext();
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        engine.put("$vtl.engine.processing_engine_names", "spark");
        engine.put("$vtl.spark.session", spark);
        return engine;
    }

    public static Bindings getBindings(Bindings input) {
        Bindings output = new SimpleBindings();
        input.forEach((k, v) -> {
            if (!k.startsWith("$")) output.put(k, v);
        });
        return output;
    }

    public static SparkConf loadSparkConfig(String stringPath) {
        try {
            SparkConf conf = new SparkConf(true);
            if (stringPath != null) {
                Path path = Path.of(stringPath, "spark.conf");
                org.apache.spark.util.Utils.loadDefaultSparkProperties(conf, path.normalize().toAbsolutePath().toString());
            }

            for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
                var normalizedName = entry.getKey().toLowerCase().replace("_", ".");
                if (normalizedName.startsWith("spark.")) {
                    conf.set(normalizedName, entry.getValue());
                }
            }

            return conf;
        } catch (Exception ex) {
            logger.error("could not load spark config from {}", stringPath, ex);
            throw ex;
        }
    }

    public static Bindings getSparkBindings(Bindings input, Integer limit) {
        Bindings output = new SimpleBindings();
        input.forEach((k, v) -> {
            if (!k.startsWith("$")) {
                Dataset<Row> sparkDs = ((SparkDataset) v).getSparkDataset();
                if (limit != null) output.put(k, new SparkDataset(sparkDs.limit(limit), Map.of()));
                else output.put(k, new SparkDataset(sparkDs, Map.of()));
            }
        });
        return output;
    }

    public static void writeSparkDatasets(Bindings bindings, Map<String, S3ForBindings> s3toSave,
                                          ObjectMapper objectMapper,
                                          SparkSession spark) {
        s3toSave.forEach((name, values) -> {
            SparkDataset dataset = (SparkDataset) bindings.get(name);
            writeSparkDataset(objectMapper, spark, values.getUrl(), dataset);
        });
    }
//
    public static void writeSparkDataset(ObjectMapper objectMapper, SparkSession spark, String location, SparkDataset dataset) {
        Dataset<Row> sparkDataset = dataset.getSparkDataset();
        sparkDataset.write().mode(SaveMode.ErrorIfExists).parquet(location + "/parquet");
        // Trick to write json thanks to spark
        String json = "";
        try {
            json = objectMapper.writeValueAsString(dataset.getDataStructure().values());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        JavaSparkContext.fromSparkContext(spark.sparkContext())
                .parallelize(List.of(json.getBytes()))
                .coalesce(1)
                .saveAsTextFile(location + "/structure.json");
    }
}
