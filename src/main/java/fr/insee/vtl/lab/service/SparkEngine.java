package fr.insee.vtl.lab.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.configuration.properties.SparkProperties;
import fr.insee.vtl.lab.model.Body;
import fr.insee.vtl.lab.model.User;
import fr.insee.vtl.lab.utils.Utils;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Service;

import javax.script.*;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Service
@ConfigurationProperties(prefix = "spark")
public class SparkEngine {

    private static final Logger logger = LogManager.getLogger(SparkEngine.class);

    private static final TypeReference<List<Structured.Component>> COMPONENT_TYPE = new TypeReference<>() {
    };

    @Autowired
    private SparkProperties sparkProperties;

    @Autowired
    private ObjectMapper objectMapper;

    public Bindings executeLocalSpark(User user, Body body) throws ScriptException {
        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master("local");

        SparkSession spark = sparkBuilder.getOrCreate();

        Bindings updatedBindings = new SimpleBindings();

        jsonBindings.forEach((k, v) -> {
            Dataset<Row> dataset = spark.read().parquet(v + "/parquet");
            try {
                List<Structured.Component> components =
                        objectMapper.readValue(Paths.get((String) v, "structure.json")
                                        .toFile(),
                                new TypeReference<>() {
                                });
                Structured.DataStructure structure = new Structured.DataStructure(components);
                updatedBindings.put(k, new SparkDataset(dataset, structure));
            } catch (IOException e) {
                logger.warn("Parquet loading failed: ", e);
            }
        });

        ScriptEngine engine = Utils.initEngineWithSpark(updatedBindings, spark);

        engine.eval(script);
        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        Bindings output = Utils.getBindings(outputBindings, true);
        return output;
    }

    public Bindings executeSparkCluster(User user, Body body) throws ScriptException {
        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master(sparkProperties.getMaster());

        sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
        sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
        sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());
        sparkBuilder.config("spark.jars", "/lib/vtl-spark.jar,/lib/vtl-model.jar");

        SparkSession spark = sparkBuilder.getOrCreate();

        Bindings updatedBindings = new SimpleBindings();

        jsonBindings.forEach((k, v) -> {
            Dataset<Row> dataset = spark.read().parquet(v + "/parquet");
            try {
                byte[] row = spark.read()
                        .format("binaryFile")
                        .load(v + "/structure.json")
                        .first()
                        .getAs("content");

                List<Structured.Component> components = objectMapper.readValue(row, COMPONENT_TYPE);
                Structured.DataStructure structure = new Structured.DataStructure(components);
                updatedBindings.put(k, new SparkDataset(dataset, structure));
            } catch (Exception e) {
                logger.warn("Parquet loading failed: ", e);
            }
        });

        ScriptEngine engine = Utils.initEngineWithSpark(updatedBindings, spark);

        engine.eval(script);
        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        Bindings output = Utils.getBindings(outputBindings, true);
        return output;
    }

}
