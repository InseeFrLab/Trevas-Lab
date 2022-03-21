package fr.insee.vtl.lab.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.model.BodyV2;
import fr.insee.vtl.lab.model.ParquetPaths;
import fr.insee.vtl.lab.model.User;
import fr.insee.vtl.lab.utils.Utils;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

import javax.script.*;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.lab.utils.Utils.loadSparkConfig;

@Service
@ConfigurationProperties(prefix = "spark")
public class SparkEngineV2 {

    private static final Logger logger = LogManager.getLogger(SparkEngineV2.class);

    private static final TypeReference<List<Structured.Component>> COMPONENT_TYPE = new TypeReference<>() {
    };

    @Autowired
    private ObjectMapper objectMapper;


    private SparkDataset readParquetDataset(SparkSession spark, String path) {
        try {
            Dataset<Row> dataset = spark.read().parquet(path + "/parquet");
            byte[] row = spark.read()
                    .format("binaryFile")
                    .load(path + "/structure.json")
                    .first()
                    .getAs("content");
            List<Structured.Component> components = objectMapper.readValue(row, COMPONENT_TYPE);
            Structured.DataStructure structure = new Structured.DataStructure(components);
            return new SparkDataset(dataset, structure);
        } catch (IOException e) {
            throw new RuntimeException("could not read file " + path, e);
        }
    }

    public Bindings executeSpark(SparkSession spark, Bindings bindings, String script) throws ScriptException {
        Bindings updatedBindings = new SimpleBindings();
        bindings.forEach((name, value) -> {
            try {
                if (value instanceof String) {
                    SparkDataset sparkDataset = readParquetDataset(spark, (String) value);
                    updatedBindings.put(name, sparkDataset);
                } else {
                    updatedBindings.put(name, value);
                }
            } catch (Exception e) {
                logger.warn("Parquet loading failed: ", e);
            }
        });

        ScriptEngine engine = Utils.initEngineWithSpark(updatedBindings, spark);

        engine.eval(script);
        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        //Bindings sizedBindings = Utils.getBindings(outputBindings, true);
        //Utils.writeSparkDatasets(dsBindings, toSave, objectMapper, spark);
        return Utils.getBindings(outputBindings);
    }

    public Bindings executeLocalSpark(User user, BodyV2 body) throws ScriptException {
        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master("local");

        SparkSession spark = sparkBuilder.getOrCreate();
        return executeSpark(spark, jsonBindings, script);
    }

    public Bindings executeSparkStatic(User user, BodyV2 body) throws ScriptException {
        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();

        SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(conf)
                .master("local");

        // Note: all the dependencies are required for deserialization.
        // See https://stackoverflow.com/questions/28079307
        sparkBuilder.config("spark.jars", String.join(",",
                "/vtl-spark.jar",
                "/vtl-model.jar",
                "/vtl-jackson.jar",
                "/vtl-parser.jar",
                "/vtl-engine.jar"
        ));

        SparkSession spark = sparkBuilder.getOrCreate();
        return executeSpark(spark, jsonBindings, script);
    }

    public Bindings executeSparkKube(User user, BodyV2 body) throws ScriptException {
        SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(conf)
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");

        // Note: all the dependencies are required for deserialization.
        // See https://stackoverflow.com/questions/28079307
        sparkBuilder.config("spark.jars", String.join(",",
                "/vtl-spark.jar",
                "/vtl-model.jar",
                "/vtl-jackson.jar",
                "/vtl-parser.jar",
                "/vtl-engine.jar"
        ));

        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();
        SparkSession spark = sparkBuilder.getOrCreate();
        return executeSpark(spark, jsonBindings, script);
    }

    public String buildParquet(User user, ParquetPaths parquetPaths) {

        String structure = parquetPaths.getStructure();
        String data = parquetPaths.getData();
        String target = parquetPaths.getTarget();

        SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(conf)
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");

        // Note: all the dependencies are required for deserialization.
        // See https://stackoverflow.com/questions/28079307
        sparkBuilder.config("spark.jars", String.join(",",
                "/vtl-spark.jar",
                "/vtl-model.jar",
                "/vtl-jackson.jar",
                "/vtl-parser.jar",
                "/vtl-engine.jar"
        ));

        SparkSession spark = sparkBuilder.getOrCreate();

        TypeReference<List<Structured.Component>> COMPONENT_TYPE = new TypeReference<>() {
        };

        byte[] row = spark.read()
                .format("binaryFile")
                .load(structure)
                .first()
                .getAs("content");

        List<Structured.Component> components = null;
        try {
            components = objectMapper.readValue(row, COMPONENT_TYPE);
        } catch (IOException e) {
            e.printStackTrace();
            return "ko";
        }
        Structured.DataStructure dsStructure = new Structured.DataStructure(components);

        StructType structType = SparkDataset.toSparkSchema(dsStructure);

        Dataset<Row> dataset = spark.read()
                .options(Map.of("header", "true", "delimiter", ";"))
                .schema(structType)
                .csv(data);
        dataset.write().mode(SaveMode.Overwrite).parquet(target);
        return "ok";
    }

}
