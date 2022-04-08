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

import javax.script.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.lab.utils.Utils.loadSparkConfig;

@Service
@ConfigurationProperties(prefix = "spark")
public class SparkEngine {

    private static final Logger logger = LogManager.getLogger(SparkEngine.class);

    private static final TypeReference<List<Structured.Component>> COMPONENT_TYPE = new TypeReference<>() {
    };

    @Autowired
    private ObjectMapper objectMapper;


    private SparkDataset readParquetDataset(SparkSession spark, S3ForBindings s3) {
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
            return new SparkDataset(dataset, structure);
        } catch (IOException e) {
            throw new RuntimeException("could not read file " + path, e);
        }
    }

    private SparkDataset readJDBCDataset(SparkSession spark, QueriesForBindings queriesForBindings) {
        // Assume we only support Postgre for now
        Dataset<Row> ds = spark.read().format("jdbc")
                .option("url", "jdbc:" + queriesForBindings.getUrl())
                .option("user", queriesForBindings.getUser())
                .option("password", queriesForBindings.getPassword())
                .option("query", queriesForBindings.getQuery())
                .option("driver", "org.postgresql.Driver")
                .load();
        return new SparkDataset(ds, Map.of());
    }

    public Bindings executeSpark(SparkSession spark, Map<String, QueriesForBindings> queriesForBindings,
                                 Map<String, S3ForBindings> s3ForBindings, String script) throws ScriptException {
        Bindings updatedBindings = new SimpleBindings();
        if (queriesForBindings != null) {
            queriesForBindings.forEach((k, v) -> {
                try {
                    SparkDataset sparkDataset = readJDBCDataset(spark, v);
                    updatedBindings.put(k, sparkDataset);
                } catch (Exception e) {
                    logger.warn("Query loading failed: ", e);
                }
            });
        }
        if (s3ForBindings != null) {
            s3ForBindings.forEach((k, v) -> {
                try {
                    SparkDataset sparkDataset = readParquetDataset(spark, v);
                    updatedBindings.put(k, sparkDataset);
                } catch (Exception e) {
                    logger.warn("S3 loading failed: ", e);
                }
            });
        }

        ScriptEngine engine = Utils.initEngineWithSpark(updatedBindings, spark);

        engine.eval(script);
        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
        //Utils.writeSparkDatasets(dsBindings, toSave, objectMapper, spark);
        return Utils.getSparkBindings(outputBindings);
    }

    public Bindings executeLocalSpark(User user, Body body) throws ScriptException {
        String script = body.getVtlScript();
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();
        Map<String, S3ForBindings> s3ForBindings = body.getS3ForBindings();

        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("vtl-lab")
                .master("local");

        SparkSession spark = sparkBuilder.getOrCreate();
        return executeSpark(spark, queriesForBindings, s3ForBindings, script);
    }

    public Bindings executeSparkStatic(User user, Body body) throws ScriptException {
        String script = body.getVtlScript();
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();
        Map<String, S3ForBindings> s3ForBindings = body.getS3ForBindings();

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
        return executeSpark(spark, queriesForBindings, s3ForBindings, script);
    }

    public Bindings executeSparkKube(User user, Body body) throws ScriptException {
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
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();
        Map<String, S3ForBindings> s3ForBindings = body.getS3ForBindings();
        SparkSession spark = sparkBuilder.getOrCreate();
        return executeSpark(spark, queriesForBindings, s3ForBindings, script);
    }

//    public String buildParquet(User user, ParquetPaths parquetPaths) {
//
//        String structure = parquetPaths.getStructure();
//        String data = parquetPaths.getData();
//        String target = parquetPaths.getTarget();
//
//        SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
//
//        SparkSession.Builder sparkBuilder = SparkSession.builder()
//                .config(conf)
//                .master("k8s://https://kubernetes.default.svc.cluster.local:443");
//
//        // Note: all the dependencies are required for deserialization.
//        // See https://stackoverflow.com/questions/28079307
//        sparkBuilder.config("spark.jars", String.join(",",
//                "/vtl-spark.jar",
//                "/vtl-model.jar",
//                "/vtl-jackson.jar",
//                "/vtl-parser.jar",
//                "/vtl-engine.jar"
//        ));
//
//        SparkSession spark = sparkBuilder.getOrCreate();
//
//        TypeReference<List<Structured.Component>> COMPONENT_TYPE = new TypeReference<>() {
//        };
//
//        byte[] row = spark.read()
//                .format("binaryFile")
//                .load(structure)
//                .first()
//                .getAs("content");
//
//        List<Structured.Component> components = null;
//        try {
//            components = objectMapper.readValue(row, COMPONENT_TYPE);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return "ko";
//        }
//        Structured.DataStructure dsStructure = new Structured.DataStructure(components);
//
//        StructType structType = SparkDataset.toSparkSchema(dsStructure);
//
//        Dataset<Row> dataset = spark.read()
//                .options(Map.of("header", "true", "delimiter", ";"))
//                .schema(structType)
//                .csv(data);
//        dataset.write().mode(SaveMode.Overwrite).parquet(target);
//        return "ok";
//    }

    public ResponseEntity<EditVisualize> getJDBC(
            User user,
            QueriesForBindings queriesForBindings) {
        SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(conf)
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");
        SparkSession spark = sparkBuilder.getOrCreate();
        Dataset<Row> ds = spark.read().format("jdbc")
                .option("url", "jdbc:" + queriesForBindings.getUrl())
                .option("user", queriesForBindings.getUser())
                .option("password", queriesForBindings.getPassword())
                .option("query", queriesForBindings.getQuery())
                .option("driver", "org.postgresql.Driver")
                .load()
                .limit(1000);

        fr.insee.vtl.model.Dataset trevasDs = new SparkDataset(ds, Map.of());

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
            S3ForBindings s3ForBindings) {
        SparkConf conf = loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .config(conf)
                .master("k8s://https://kubernetes.default.svc.cluster.local:443");
        SparkSession spark = sparkBuilder.getOrCreate();

        EditVisualize editVisualize = new EditVisualize();

        String path = s3ForBindings.getUrl();
        try {
            Dataset<Row> dataset = spark.read().parquet(path + "/parquet");
            byte[] row = spark.read()
                    .format("binaryFile")
                    .load(path + "/structure.json")
                    .first()
                    .getAs("content");
            List<Structured.Component> components = objectMapper.readValue(row, COMPONENT_TYPE);
            Structured.DataStructure builtStructure = new Structured.DataStructure(components);
            fr.insee.vtl.model.Dataset trevasDs = new SparkDataset(dataset, builtStructure);

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
        } catch (IOException e) {
            throw new RuntimeException("could not read file " + path, e);
        }
        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }
}