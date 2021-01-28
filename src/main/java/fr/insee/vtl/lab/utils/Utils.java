package fr.insee.vtl.lab.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import javax.script.*;

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

    public static Bindings getBindings(Bindings input, Boolean toLength) {
        if (!toLength) return input;
        Bindings output = new SimpleBindings();
        input.forEach((k, v) -> {
            if (!k.startsWith("$")) {
                var size = ((fr.insee.vtl.model.Dataset) v).getDataPoints().size();
                output.put(k, size);
                logger.info(k + " dataset has size: " + size);
            }
        });
        return output;
    }

    public static void write(Bindings bindings, Bindings toSave, SparkSession spark) {
        toSave.forEach((k, v) -> {
            SparkDataset dataset = (SparkDataset) bindings.get(k);
            Dataset<Row> sparkDataset = dataset.getSparkDataset();
            sparkDataset.write().mode(SaveMode.ErrorIfExists).parquet(v + "/parquet");
            // TODO: Handle write of structure.json
        });
    }
}
