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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static fr.insee.vtl.lab.utils.Utils.*;

@Service
@ConfigurationProperties(prefix = "spark")
public class SparkEngine {

    private static final Logger logger = LogManager.getLogger(SparkEngine.class);

    private static final TypeReference<List<Structured.Component>> COMPONENT_TYPE = new TypeReference<>() {
    };

    @Autowired
    private ObjectMapper objectMapper;

    private SparkSession buildSparkSession(ExecutionType type, Boolean addJars) throws Exception {
        SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab");
        if (ExecutionType.LOCAL == type) {
            sparkBuilder
                    .config(conf)
                    .master("local");
            return sparkBuilder.getOrCreate();
        } else if (ExecutionType.CLUSTER_STATIC == type || ExecutionType.CLUSTER_KUBERNETES == type) {
            if (addJars) {
                // Note: all the dependencies are required for deserialization.
                // See https://stackoverflow.com/questions/28079307
                conf.set("spark.jars.packages", String.join(",",
                        "/vtl-spark.jar",
                        "/vtl-model.jar",
                        "/vtl-parser.jar",
                        "/vtl-engine.jar",
                        "/postgresql.jar"
                ));
            }
            if (ExecutionType.CLUSTER_KUBERNETES == type) {
                sparkBuilder
                        .master("k8s://https://kubernetes.default.svc.cluster.local:443");
            }
            sparkBuilder.config(conf);
            return sparkBuilder.getOrCreate();
        } else throw new Exception("Unknow execution type: " + type);
    }

    private SparkDataset readParquetDataset(SparkSession spark, S3ForBindings s3, Integer limit) throws Exception {
        String path = s3.getUrl();
        Dataset<Row> dataset;
        Dataset<Row> json;
        try {
            dataset = spark.read().parquet(path + "/data");
            json = spark.read()
                    .option("multiLine", "true")
                    .json(path + "/structure");
        } catch (Exception e) {
            throw new Exception("An error has occured while loading: " + path);
        }
        Map<String, fr.insee.vtl.model.Dataset.Role> components = json.collectAsList().stream().map(r -> {
                            String name = r.getAs("name");
                            Class type = r.getAs("type").getClass();
                            fr.insee.vtl.model.Dataset.Role role = fr.insee.vtl.model.Dataset.Role.valueOf(r.getAs("role"));
                            return new Structured.Component(name, type, role);
                        }
                ).collect(Collectors.toList())
                .stream()
                .collect(Collectors.toMap(Structured.Component::getName, Structured.Component::getRole));
        if (limit != null) return new SparkDataset(dataset.limit(limit), components);
        return new SparkDataset(dataset, components);
    }

    private SparkDataset readJDBCDataset(SparkSession spark, QueriesForBindings queriesForBindings, Integer limit) {
        String jdbcPrefix = "";
        try {
            jdbcPrefix = getJDBCPrefix(queriesForBindings.getDbtype());
        } catch (Exception e) {
            e.printStackTrace();
        }
        Dataset<Row> ds = spark.read().format("jdbc")
                .option("url", jdbcPrefix + queriesForBindings.getUrl())
                .option("user", queriesForBindings.getUser())
                .option("password", queriesForBindings.getPassword())
                .option("query", queriesForBindings.getQuery())
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

        Map<String, S3ForBindings> s3ToSave = body.getToSave().getS3ForBindings();
        if (null != s3ToSave) {
            writeSparkDatasets(outputBindings, s3ToSave, objectMapper, spark);
        }

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
            rowMap.put("type", component.getType().getSimpleName());
            rowMap.put("role", component.getRole());
            structure.add(rowMap);
        });
        editVisualize.setDataStructure(structure);
        editVisualize.setDataPoints(trevasDs.getDataAsList());
        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }
}