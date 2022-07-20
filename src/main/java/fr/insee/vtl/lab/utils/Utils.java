package fr.insee.vtl.lab.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.model.QueriesForBindingsToSave;
import fr.insee.vtl.lab.model.Role;
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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                logger.warn("Using spark.conf is deprecated");
                Path path = Path.of(stringPath, "spark.conf");
                org.apache.spark.util.Utils.loadDefaultSparkProperties(conf, path.normalize().toAbsolutePath().toString());
            }

            for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
                var normalizedName = entry.getKey().toLowerCase().replace("_", ".");
                if (normalizedName.startsWith("spark.")) {
                    // TODO: find a better way to handle spark props
                    if (normalizedName.contains("dynamicallocation")) {
                        normalizedName = normalizedName.replace("dynamicallocation", "dynamicAllocation");
                    }
                    if (normalizedName.contains("shuffletracking")) {
                        normalizedName = normalizedName.replace("shuffletracking", "shuffleTracking");
                    }
                    if (normalizedName.contains("minexecutors")) {
                        normalizedName = normalizedName.replace("minexecutors", "minExecutors");
                    }
                    if (normalizedName.contains("maxexecutors")) {
                        normalizedName = normalizedName.replace("maxexecutors", "maxExecutors");
                    }
                    if (normalizedName.contains("extrajavaoptions")) {
                        normalizedName = normalizedName.replace("extrajavaoptions", "extraJavaOptions");
                    }
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
                if (v instanceof SparkDataset) {
                    Dataset<Row> sparkDs = ((SparkDataset) v).getSparkDataset();
                    if (limit != null) output.put(k, new SparkDataset(sparkDs.limit(limit), Map.of()));
                    else output.put(k, new SparkDataset(sparkDs, Map.of()));
                } else output.put(k, v);
            }
        });
        return output;
    }

    public static void writeSparkDatasetsJDBC(Bindings bindings,
                                              Map<String, QueriesForBindingsToSave> queriesForBindingsToSave,
                                              ObjectMapper objectMapper,
                                              SparkSession spark) {
        queriesForBindingsToSave.forEach((name, values) -> {
            SparkDataset dataset = (SparkDataset) bindings.get(name);
            Dataset<Row> dsSpark = dataset.getSparkDataset();
            String jdbcPrefix = "";
            try {
                jdbcPrefix = getJDBCPrefix(values.getDbtype());
            } catch (Exception e) {
                e.printStackTrace();
            }
            dsSpark.write()
                    .mode(SaveMode.Overwrite)
                    .format("jdbc")
                    .option("url", jdbcPrefix + values.getUrl())
                    .option("dbtable", values.getTable())
                    .option("user", values.getUser())
                    .option("password", values.getPassword())
                    .save();
            String rolesUrl = values.getRoleUrl();
            if (rolesUrl == null || !rolesUrl.equals("")) {
                // Trick to write json thanks to spark
                String json = "";
                try {
                    json = objectMapper.writeValueAsString(dataset.getDataStructure().values());
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
                JavaSparkContext.fromSparkContext(spark.sparkContext())
                        .parallelize(List.of(json))
                        .coalesce(1)
                        .saveAsTextFile(values.getRoleUrl());
            }
        });
    }

    public static void writeSparkS3Datasets(Bindings bindings, Map<String, S3ForBindings> s3toSave,
                                            ObjectMapper objectMapper,
                                            SparkSession spark) {
        s3toSave.forEach((name, values) -> {
            SparkDataset dataset = (SparkDataset) bindings.get(name);
            writeSparkDataset(objectMapper, spark, values.getUrl(), dataset);
        });
    }

    public static void writeSparkDataset(ObjectMapper objectMapper, SparkSession spark, String location, SparkDataset dataset) {
        Dataset<Row> sparkDataset = dataset.getSparkDataset();
        sparkDataset.write().mode(SaveMode.Overwrite).parquet(location + "/data");
        // Trick to write json thanks to spark
        String json = "";
        try {
            json = objectMapper.writeValueAsString(dataset.getDataStructure().values());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        JavaSparkContext.fromSparkContext(spark.sparkContext())
                .parallelize(List.of(json))
                .coalesce(1)
                .saveAsTextFile(location + "/structure");
    }

    public static String getJDBCPrefix(String dbType) throws Exception {
        if (dbType.equals("postgre")) return "jdbc:postgresql://";
        throw new Exception("Unsupported dbtype: " + dbType);
    }

    public static Map<String, fr.insee.vtl.model.Dataset.Role> getRoles(String url, ObjectMapper objectMapper) throws SQLException {
        List<Role> roles = null;
        if (null != url) {
            try {
                roles = objectMapper.readValue(
                        new URL(url),
                        objectMapper.getTypeFactory().constructCollectionType(List.class, Role.class));
            } catch (JsonProcessingException | MalformedURLException e) {
                throw new SQLException("Error while fetching roles");
            } catch (IOException e) {
                throw new SQLException("Role URL malformed");
            }
        }
        return roles.stream()
                .collect(Collectors.toMap(
                        Role::getName,
                        Role::getRole
                ));
    }
}
