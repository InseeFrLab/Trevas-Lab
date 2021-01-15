package fr.insee.vtl.lab.utils;

import fr.insee.vtl.engine.VtlScriptEngine;
import fr.insee.vtl.engine.VtlScriptEngineFactory;
import fr.insee.vtl.model.Dataset;
import org.apache.spark.sql.SparkSession;

import javax.script.*;

public class Utils {

    public static ScriptEngine initEngine(Bindings bindings) {
        ScriptEngine engine = new VtlScriptEngine(new VtlScriptEngineFactory());
        ScriptContext context = engine.getContext();
        context.setBindings(bindings, ScriptContext.ENGINE_SCOPE);
        return engine;
    }

    public static ScriptEngine initEngineWithSpark(Bindings bindings, SparkSession spark) {
        ScriptEngine engine = new VtlScriptEngine(new VtlScriptEngineFactory());
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
        Bindings output = new SimpleBindings();
        input.forEach((k, v) -> {
            if (!k.startsWith("$")) output.put(k, ((Dataset) v).getDataPoints().size());
        });
        return output;
    }
}
