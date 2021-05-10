package fr.insee.vtl.lab.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.configuration.properties.SparkProperties;
import fr.insee.vtl.lab.model.Body;
import fr.insee.vtl.lab.model.ParquetPaths;
import fr.insee.vtl.lab.model.User;
import fr.insee.vtl.lab.utils.Utils;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

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
        Bindings toSave = body.getToSave();

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
        Bindings dsBindings = Utils.getBindings(outputBindings);
        Bindings sizedBindings = Utils.getBindings(outputBindings, true);
        Utils.writeSparkDataset(dsBindings, toSave, objectMapper, spark);
        return sizedBindings;
    }

    public Bindings executeSparkStatic(User user, Body body) throws ScriptException {
        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();
        Bindings toSave = body.getToSave();

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master(sparkProperties.getMaster());

        sparkBuilder.config("spark.dynamicAllocation.enabled", sparkProperties.getDynamicAllocationEnabled());
        sparkBuilder.config("spark.dynamicAllocation.shuffleTracking.enabled", sparkProperties.getDynamicAllocationEnabled());
        sparkBuilder.config("spark.dynamicAllocation.minExecutors", sparkProperties.getDynamicAllocationMinExecutors());


        sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
        sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
        sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());
        sparkBuilder.config("spark.hadoop.fs.s3a.path.style.access", true);
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

        Bindings updatedBindings = new SimpleBindings();

        jsonBindings.forEach((k, v) -> {
            try {
                Dataset<Row> dataset = spark.read().parquet(v + "/parquet");
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
        Bindings dsBindings = Utils.getBindings(outputBindings);
        Bindings sizedBindings = Utils.getBindings(outputBindings, true);
        Utils.writeSparkDataset(dsBindings, toSave, objectMapper, spark);
        return sizedBindings;
    }

    public Bindings executeSparkKube(User user, Body body) throws ScriptException {
        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();
        Bindings toSave = body.getToSave();

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");

        sparkBuilder.config("spark.kubernetes.container.image.pullPolicy", sparkProperties.getKubernetesContainerImagePullPolicy());
        sparkBuilder.config("spark.kubernetes.container.image", sparkProperties.getKubernetesContainerImage());

        sparkBuilder.config("spark.dynamicAllocation.enabled", sparkProperties.getDynamicAllocationEnabled());
        sparkBuilder.config("spark.dynamicAllocation.shuffleTracking.enabled", sparkProperties.getDynamicAllocationEnabled());
        sparkBuilder.config("spark.dynamicAllocation.minExecutors", sparkProperties.getDynamicAllocationMinExecutors());

        sparkBuilder.config("spark.driver.memory", sparkProperties.getDriverMemory());
        sparkBuilder.config("spark.executor.memory", sparkProperties.getExecutorMemory());
        sparkBuilder.config("spark.rpc.message.maxSize", 2046);

        sparkBuilder.config("spark.kubernetes.namespace", sparkProperties.getKubernetesNamespace());
        sparkBuilder.config("spark.kubernetes.executor.request.cores", sparkProperties.getKubernetesExecutorRequestCores());
        sparkBuilder.config("spark.kubernetes.driver.pod.name", sparkProperties.getKubernetesDriverPodName());

        sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
        sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
        sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());
        sparkBuilder.config("spark.hadoop.fs.s3a.path.style.access", true);
        sparkBuilder.config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

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
        Bindings dsBindings = Utils.getBindings(outputBindings);
        Bindings sizedBindings = Utils.getBindings(outputBindings, true);
        Utils.writeSparkDataset(dsBindings, toSave, objectMapper, spark);
        return sizedBindings;
    }

    public String buildParquet(User user, ParquetPaths parquetPaths) {

        String structure = parquetPaths.getStructure();
        String data = parquetPaths.getData();
        String target = parquetPaths.getTarget();

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");

        sparkBuilder.config("spark.kubernetes.container.image.pullPolicy", sparkProperties.getKubernetesContainerImagePullPolicy());
        sparkBuilder.config("spark.kubernetes.container.image", sparkProperties.getKubernetesContainerImage());

        sparkBuilder.config("spark.dynamicAllocation.enabled", sparkProperties.getDynamicAllocationEnabled());
        sparkBuilder.config("spark.dynamicAllocation.shuffleTracking.enabled", sparkProperties.getDynamicAllocationEnabled());
        sparkBuilder.config("spark.dynamicAllocation.minExecutors", sparkProperties.getDynamicAllocationMinExecutors());

        sparkBuilder.config("spark.driver.memory", sparkProperties.getDriverMemory());
        sparkBuilder.config("spark.executor.memory", sparkProperties.getExecutorMemory());
        sparkBuilder.config("spark.rpc.message.maxSize", 2046);

        sparkBuilder.config("spark.kubernetes.namespace", sparkProperties.getKubernetesNamespace());
        sparkBuilder.config("spark.kubernetes.executor.request.cores", sparkProperties.getKubernetesExecutorRequestCores());
        sparkBuilder.config("spark.kubernetes.driver.pod.name", sparkProperties.getKubernetesDriverPodName());

        sparkBuilder.config("spark.hadoop.fs.s3a.access.key", sparkProperties.getAccessKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.secret.key", sparkProperties.getSecretKey());
        sparkBuilder.config("spark.hadoop.fs.s3a.connection.ssl.enabled", sparkProperties.getSslEnabled());
        sparkBuilder.config("spark.hadoop.fs.s3a.session.token", sparkProperties.getSessionToken());
        sparkBuilder.config("spark.hadoop.fs.s3a.endpoint", sparkProperties.getSessionEndpoint());
        sparkBuilder.config("spark.hadoop.fs.s3a.path.style.access", true);
        sparkBuilder.config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

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
                .options(Map.of("header", "true", "delimiter",";"))
                .schema(structType)
                .csv(data);
        dataset.write().mode(SaveMode.Overwrite).parquet(target);
        return "ok";
    }

}
