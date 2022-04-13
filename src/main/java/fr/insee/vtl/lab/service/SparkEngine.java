package fr.insee.vtl.lab.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.model.*;
import fr.insee.vtl.lab.utils.Utils;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.lab.utils.Utils.loadSparkConfig;
import static fr.insee.vtl.lab.utils.Utils.writeSparkDatasets;

@Service
@ConfigurationProperties(prefix = "spark")
public class SparkEngine {

    private static final Logger logger = LogManager.getLogger(SparkEngine.class);

    private static final TypeReference<List<Structured.Component>> COMPONENT_TYPE = new TypeReference<>() {
    };

    @Autowired
    private ObjectMapper objectMapper;

    private SparkSession buildSparkSession(ExecutionType type, Boolean addJars) throws Exception {
        if (ExecutionType.LOCAL == type) {
            SparkSession.Builder sparkBuilder = SparkSession.builder()
                    .appName("vtl-lab")
                    .master("local");
            return sparkBuilder.getOrCreate();
        } else if (ExecutionType.CLUSTER_STATIC == type || ExecutionType.CLUSTER_KUBERNETES == type) {
            SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
            SparkSession.Builder sparkBuilder = SparkSession.builder()
                    .config(conf);
            if (addJars) {
                // Note: all the dependencies are required for deserialization.
                // See https://stackoverflow.com/questions/28079307
                sparkBuilder.config("spark.jars", String.join(",",
                        "/vtl-spark.jar",
                        "/vtl-model.jar",
                        "/vtl-jackson.jar",
                        "/vtl-parser.jar",
                        "/vtl-engine.jar"
                ));
                // Add JDBC Driver Jars
                sparkBuilder.config("spark.jars.package", "org.postgresql:postgresql:42.3.3");
            }
            if (ExecutionType.CLUSTER_KUBERNETES == type)
                sparkBuilder.master("k8s://https://kubernetes.default.svc.cluster.local:443");
            return sparkBuilder.getOrCreate();
        } else throw new Exception("Unknow execution type: " + type);
    }

    private SparkDataset readParquetDataset(SparkSession spark, S3ForBindings s3, Integer limit) {
        String path = s3.getUrl();
        try {
            Dataset<Row> dataset = spark.read().parquet(path + "/parquet");
            byte[] row = spark.read()
                    .format("binaryFile")
                    .load(path + "/structure.json")
                    .first()
                    .getAs("content");
            List<Structured.Component> components = objectMapper.readValue(row, COMPONENT_TYPE);
            Structured.DataStructure structure = new Structured.DataStructure(components);
            if (limit != null) return new SparkDataset(dataset.limit(limit), structure);
            return new SparkDataset(dataset, structure);
        } catch (IOException e) {
            throw new RuntimeException("could not read file " + path, e);
        }
    }

    private SparkDataset readJDBCDataset(SparkSession spark, QueriesForBindings queriesForBindings, Integer limit) {
        // Assume we only support Postgre for now
        Dataset<Row> ds = spark.read().format("jdbc")
                .option("url", "jdbc:postgresql://" + queriesForBindings.getUrl())
                .option("user", queriesForBindings.getUser())
                .option("password", queriesForBindings.getPassword())
                .option("query", queriesForBindings.getQuery())
                .option("driver", "org.postgresql.Driver")
                .load();
        if (limit != null) return new SparkDataset(ds.limit(limit), Map.of());
        return new SparkDataset(ds, Map.of());
    }

    public Bindings executeSpark(User user, Body body, ExecutionType type) throws Exception {
        String script = body.getVtlScript();
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();
        Map<String, S3ForBindings> s3ForBindings = body.getS3ForBindings();

        SparkSession spark = buildSparkSession(type, true);

        Bindings bindings = new SimpleBindings();

        if (queriesForBindings != null) {
            queriesForBindings.forEach((k, v) -> {
                try {
                    SparkDataset sparkDataset = readJDBCDataset(spark, v, null);
                    bindings.put(k, sparkDataset);
                } catch (Exception e) {
                    logger.warn("Query loading failed: ", e);
                }
            });
        }
        if (s3ForBindings != null) {
            s3ForBindings.forEach((k, v) -> {
                try {
                    SparkDataset sparkDataset = readParquetDataset(spark, v, null);
                    bindings.put(k, sparkDataset);
                } catch (Exception e) {
                    logger.warn("S3 loading failed: ", e);
                }
            });
        }

        ScriptEngine engine = Utils.initEngineWithSpark(bindings, spark);

        engine.eval(script);
        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);

        writeSparkDatasets(outputBindings, body.getToSave().getS3ForBindings(), objectMapper, spark);

        return Utils.getSparkBindings(outputBindings, 1000);
    }

    public ResponseEntity<EditVisualize> getJDBC(
            User user,
            QueriesForBindings queriesForBindings,
            ExecutionType type) throws Exception {

        SparkSession spark = buildSparkSession(type, false);

        fr.insee.vtl.model.Dataset trevasDs = readJDBCDataset(spark, queriesForBindings, 1000);

        EditVisualize editVisualize = new EditVisualize();

        List<Map<String, Object>> structure = new ArrayList<>();
        trevasDs.getDataStructure().entrySet().forEach(e -> {
            Structured.Component component = e.getValue();
            Map<String, Object> row = new HashMap<>();
            row.put("name", component.getName());
            row.put("type", component.getType());
            structure.add(row);
        });
        editVisualize.setDataStructure(structure);

        editVisualize.setDataPoints(trevasDs.getDataAsList());

        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }

    public ResponseEntity<EditVisualize> getS3(
            User user,
            S3ForBindings s3ForBindings,
            ExecutionType type) throws Exception {

        SparkSession spark = buildSparkSession(type, false);

        EditVisualize editVisualize = new EditVisualize();

        fr.insee.vtl.model.Dataset trevasDs = readParquetDataset(spark, s3ForBindings, 1000);

        List<Map<String, Object>> structure = new ArrayList<>();
        trevasDs.getDataStructure().entrySet().forEach(e -> {
            Structured.Component component = e.getValue();
            Map<String, Object> rowMap = new HashMap<>();
            rowMap.put("name", component.getName());
            rowMap.put("type", component.getType());
            structure.add(rowMap);
        });
        editVisualize.setDataStructure(structure);
        editVisualize.setDataPoints(trevasDs.getDataAsList());
        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }
}