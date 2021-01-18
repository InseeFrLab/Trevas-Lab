package fr.insee.vtl.lab.service;

import fr.insee.vtl.lab.model.Body;
import fr.insee.vtl.lab.model.User;
import fr.insee.vtl.lab.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;

@Service
public class InMemoryEngine {

    private static final Logger logger = LogManager.getLogger(InMemoryEngine.class);

    public Bindings executeInMemory(User user, Body body) {
        String script = body.getVtlScript();
        Bindings jsonBindings = body.getBindings();

        ScriptEngine engine = Utils.initEngine(jsonBindings);

        try {
            engine.eval(script);
            Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
            Bindings output = Utils.getBindings(outputBindings);
            return output;
        } catch (Exception e) {
            logger.warn("Eval failed: ", e);
        }
        return jsonBindings;
    }

}
