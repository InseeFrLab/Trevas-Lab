package fr.insee.trevas.lab.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.trevas.lab.model.*;
import fr.insee.trevas.lab.utils.Utils;
import fr.insee.vtl.model.Structured;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameReader;
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

@Service
@ConfigurationProperties(prefix = "spark")
public class SparkEngine {

    private static final Logger logger = LogManager.getLogger(SparkEngine.class);

    @Autowired
    private ObjectMapper objectMapper;

    private SparkSession buildSparkSession() {
        SparkConf conf = Utils.loadSparkConfig(System.getenv("SPARK_CONF_DIR"));
        conf.set("spark.driver.allowMultipleContexts", "true");
        // Note: all the dependencies are required for deserialization.
        // See https://stackoverflow.com/questions/28079307
        conf.set("spark.jars", String.join(",",
                "/vtl-spark.jar",
                "/vtl-model.jar",
                "/vtl-parser.jar",
                "/vtl-engine.jar",
                "/vtl-jackson.jar"
        ));
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("trevas-lab");
        if (!conf.contains("spark.master")) {
            conf.set("spark.master", "local");
        }
        sparkBuilder.config(conf);
        return sparkBuilder.getOrCreate();
    }

    private SparkDataset readS3Dataset(SparkSession spark, S3ForBindings s3, Integer limit) throws Exception {
        String path = s3.getUrl();
        String fileType = s3.getFiletype();
        Dataset<Row> dataset;
        try {
            if ("csv".equals(fileType))
                dataset = spark.read()
                        .option("delimiter", ";")
                        .option("header", "true")
                        .csv(path);
            else if ("parquet".equals(fileType)) dataset = spark.read().parquet(path);
            else if ("sas".equals(fileType)) dataset = spark.read()
                    .format("com.github.saurfang.sas.spark")
                    .load(path);
            else throw new Exception("Unknow S3 file type: " + fileType);
        } catch (Exception e) {
            throw new Exception("An error has occured while loading: " + path);
        }
        // Explore "take" for efficiency (returns rows)
        if (limit != null) dataset = dataset.limit(limit);
        return new SparkDataset(dataset);
    }

    private SparkDataset readJDBCDataset(SparkSession spark, QueriesForBindings queriesForBindings, Integer limit) throws Exception {
        String jdbcPrefix = "";
        String dbType = queriesForBindings.getDbtype();
        try {
            jdbcPrefix = Utils.getJDBCPrefix(dbType);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        }
        DataFrameReader dfReader = spark.read().format("jdbc")
                .option("url", jdbcPrefix + queriesForBindings.getUrl())
                .option("user", queriesForBindings.getUser())
                .option("password", queriesForBindings.getPassword())
                .option("query", queriesForBindings.getQuery());
        if (dbType.equals("postgre")) {
            dfReader.option("driver", "net.postgis.jdbc.DriverWrapper")
                    .option("driver", "org.postgresql.Driver");
        }
        if (dbType.equals("mariadb")) {
            dfReader.option("driver", "com.mysql.cj.jdbc.Driver");
        }
        Dataset<Row> dataset = dfReader.load();
        // Explore "take" for efficiency (returns rows)
        if (limit != null) dataset = dataset.limit(limit);
        return new SparkDataset(dataset);
    }

    public Bindings executeSpark(User user, Body body, Boolean preview) throws Exception {
        String script = body.getVtlScript();
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();
        Map<String, S3ForBindings> s3ForBindings = body.getS3ForBindings();

        SparkSession spark = buildSparkSession();

        Bindings bindings = new SimpleBindings();

        Integer limit = preview ? 0 : null;

        if (queriesForBindings != null) {
            queriesForBindings.forEach((k, v) -> {
                try {

                    SparkDataset sparkDataset = readJDBCDataset(spark, v, limit);
                    bindings.put(k, sparkDataset);
                } catch (Exception e) {
                    logger.warn("Query loading failed: ", e);
                }
            });
        }
        if (s3ForBindings != null) {
            s3ForBindings.forEach((k, v) -> {
                try {
                    SparkDataset sparkDataset = readS3Dataset(spark, v, limit);
                    bindings.put(k, sparkDataset);
                } catch (Exception e) {
                    logger.warn("S3 loading failed: ", e);

                }
            });
        }

        ScriptEngine engine = Utils.initEngineWithSpark(bindings, spark);

        try {
            engine.eval(script);
        } catch (Exception e) {
            throw new Exception(e);
        }
        Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);

        Map<String, QueriesForBindingsToSave> queriesForBindingsToSave = body.getToSave().getJdbcForBindingsToSave();
        if (null != queriesForBindingsToSave) {
            Utils.writeSparkDatasetsJDBC(outputBindings, queriesForBindingsToSave);
        }

        Map<String, S3ForBindings> s3ToSave = body.getToSave().getS3ForBindings();
        if (null != s3ToSave) {
            Utils.writeSparkS3Datasets(outputBindings, s3ToSave, objectMapper, spark);
        }

        return Utils.getSparkBindings(outputBindings, 100);
    }

    public ResponseEntity<EditVisualize> getJDBC(
            User user,
            QueriesForBindings queriesForBindings) throws Exception {

        SparkSession spark = buildSparkSession();

        fr.insee.vtl.model.Dataset trevasDs = readJDBCDataset(spark, queriesForBindings, 100);

        EditVisualize editVisualize = new EditVisualize();

        List<Map<String, Object>> structure = new ArrayList<>();

        trevasDs.getDataStructure().entrySet().forEach(e -> {
            Structured.Component component = e.getValue();
            Map<String, Object> row = new HashMap<>();
            row.put("name", component.getName());
            row.put("type", component.getType().getSimpleName());
            // Default has to be handled by Trevas
            row.put("role", "MEASURE");
            structure.add(row);
        });
        editVisualize.setDataStructure(structure);

        editVisualize.setDataPoints(trevasDs.getDataAsList());

        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }

    public ResponseEntity<EditVisualize> getS3(
            User user,
            S3ForBindings s3ForBindings) throws Exception {

        SparkSession spark = buildSparkSession();

        EditVisualize editVisualize = new EditVisualize();

        fr.insee.vtl.model.Dataset trevasDs = readS3Dataset(spark, s3ForBindings, 100);

        List<Map<String, Object>> structure = new ArrayList<>();
        trevasDs.getDataStructure().entrySet().forEach(e -> {
            Structured.Component component = e.getValue();
            Map<String, Object> rowMap = new HashMap<>();
            rowMap.put("name", component.getName());
            rowMap.put("type", component.getType().getSimpleName());
            rowMap.put("role", component.getRole().toString());
            structure.add(rowMap);
        });
        editVisualize.setDataStructure(structure);
        editVisualize.setDataPoints(trevasDs.getDataAsList());
        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }

}