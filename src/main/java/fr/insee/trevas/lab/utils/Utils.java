package fr.insee.trevas.lab.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.trevas.lab.model.QueriesForBindingsToSave;
import fr.insee.trevas.lab.model.S3ForBindings;
import fr.insee.vtl.model.InMemoryDataset;
import fr.insee.vtl.model.PersistentDataset;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.script.*;
import java.nio.file.Path;
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
            if (!k.startsWith("$")) {
                if (v instanceof PersistentDataset) {
                    output.put(k + "$PersistentDataset", v);
                } else {
                    output.put(k, v);
                }
            }
            ;
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
                if (entry.getKey().startsWith("spark.")) {
                    conf.set(entry.getKey(), entry.getValue());
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
                if ((v instanceof PersistentDataset) || (v instanceof SparkDataset)) {
                    String name = k;
                    SparkDataset spDs = null;
                    if (v instanceof PersistentDataset) {
                        fr.insee.vtl.model.Dataset ds = ((PersistentDataset) v).getDelegate();
                        name = name + "$PersistentDataset";
                        spDs = (SparkDataset) ds;
                    }
                    if (v instanceof SparkDataset) {
                        spDs = (SparkDataset) v;
                    }
                    Dataset<Row> sparkDs = (spDs).getSparkDataset();
                    if (limit != null) {
                        SparkDataset sparkDataset = new SparkDataset(sparkDs.limit(limit));
                        InMemoryDataset im = new InMemoryDataset(
                                sparkDataset.getDataPoints(),
                                sparkDataset.getDataStructure());
                        output.put(name, im);
                    } else output.put(name, new SparkDataset(sparkDs)); // useless
                }
            }
        });
        return output;
    }

    public static void writeSparkDatasetsJDBC(Bindings bindings,
                                              Map<String, QueriesForBindingsToSave> queriesForBindingsToSave
    ) {
        queriesForBindingsToSave.forEach((name, values) -> {
            if (!bindings.containsKey(name)) {
                try {
                    throw new Exception(name + " is not defined in script");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            Object ds = bindings.get(name);
            if (!(ds instanceof PersistentDataset)) {
                try {
                    throw new Exception(name + " is not a Persistent datatset (affect it with \"<-\")");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            fr.insee.vtl.model.Dataset dataset = ((PersistentDataset) bindings.get(name)).getDelegate();
            if (dataset instanceof SparkDataset) {
                String jdbcPrefix = "";
                try {
                    Dataset<Row> dsSpark = ((SparkDataset) dataset).getSparkDataset();
                    jdbcPrefix = getJDBCPrefix(values.getDbtype());
                    dsSpark.write()
                            .mode(SaveMode.Overwrite)
                            .format("jdbc")
                            .option("url", jdbcPrefix + values.getUrl())
                            .option("dbtable", values.getTable())
                            .option("user", values.getUser())
                            .option("password", values.getPassword())
                            .save();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void writeSparkS3Datasets(Bindings bindings, Map<String, S3ForBindings> s3toSave,
                                            ObjectMapper objectMapper,
                                            SparkSession spark) {
        s3toSave.forEach((name, values) -> {
            Object ds = bindings.get(name);
            if (!(ds instanceof PersistentDataset)) {
                try {
                    throw new Exception(name + " is not a Persistent datatset (affect it with \"<-\")");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            fr.insee.vtl.model.Dataset dataset = ((PersistentDataset) bindings.get(name)).getDelegate();
            if (dataset instanceof SparkDataset) {
                try {
                    writeSparkDataset(objectMapper, spark, values, (SparkDataset) dataset);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static void writeSparkDataset(ObjectMapper objectMapper, SparkSession spark, S3ForBindings s3, SparkDataset dataset) throws Exception {
        Dataset<Row> sparkDataset = dataset.getSparkDataset();
        String path = s3.getUrl();
        String fileType = s3.getFiletype();
        if ("csv".equals(fileType))
            sparkDataset.write()
                    .mode(SaveMode.Overwrite)
                    .option("delimiter", ";")
                    .option("header", "true")
                    .csv(path);
        else if ("parquet".equals(fileType))
            sparkDataset.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(path);
        else throw new Exception("Unknow S3 file type: " + fileType);
    }

    public static String getJDBCPrefix(String dbType) throws Exception {
        if (dbType.equals("postgre")) return "jdbc:postgresql://";
        if (dbType.equals("mariadb")) return "jdbc:mysql://";
        throw new Exception("Unsupported dbtype: " + dbType);
    }

}
