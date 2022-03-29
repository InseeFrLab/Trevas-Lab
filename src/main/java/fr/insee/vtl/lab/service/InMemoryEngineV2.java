package fr.insee.vtl.lab.service;

import fr.insee.vtl.jdbc.JDBCDataset;
import fr.insee.vtl.lab.model.BodyV2;
import fr.insee.vtl.lab.model.QueriesForBindings;
import fr.insee.vtl.lab.model.User;
import fr.insee.vtl.lab.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class InMemoryEngineV2 {

    private static final Logger logger = LogManager.getLogger(InMemoryEngineV2.class);

    public Bindings executeInMemory(User user, BodyV2 body) {
        String script = body.getVtlScript();
        Bindings bindings = body.getBindings();
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();

        AtomicReference<Connection> connection = null;
        AtomicReference<Statement> statement = null;

        if (queriesForBindings != null) {
            queriesForBindings.forEach((k, v) -> {
                try {
                    Class.forName("org.postgresql.Driver");
                    connection.set(DriverManager.getConnection(
                            "jdbc:" + v.getUrl(),
                            v.getUser(),
                            v.getPassword()));
                    statement.set(connection.get().createStatement());
                } catch (SQLException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
                JDBCDataset jdbcDataset = new JDBCDataset(() -> {
                    try {
                        return statement.get().executeQuery(v.getQuery());
                    } catch (SQLException se) {
                        throw new RuntimeException(se);
                    }
                });
                bindings.put(k, jdbcDataset);
            });
        }

        ScriptEngine engine = Utils.initEngine(bindings);

        try {
            engine.eval(script);
            Bindings outputBindings = engine.getContext().getBindings(ScriptContext.ENGINE_SCOPE);
            Bindings output = Utils.getBindings(outputBindings);
            return output;
        } catch (Exception e) {
            logger.warn("Eval failed: ", e);
        }
        return bindings;
    }

}
