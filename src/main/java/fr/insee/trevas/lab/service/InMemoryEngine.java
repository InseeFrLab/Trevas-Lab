package fr.insee.trevas.lab.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.trevas.lab.model.Body;
import fr.insee.trevas.lab.model.EditVisualize;
import fr.insee.trevas.lab.model.QueriesForBindings;
import fr.insee.trevas.lab.model.User;
import fr.insee.trevas.lab.utils.Utils;
import fr.insee.vtl.jdbc.JDBCDataset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class InMemoryEngine {

    private static final Logger logger = LogManager.getLogger(InMemoryEngine.class);

    @Autowired
    private ObjectMapper objectMapper;

    public Bindings executeInMemory(User user, Body body) throws Exception {
        String script = body.getVtlScript();
        Bindings bindings = body.getBindings();
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();

        if (queriesForBindings != null) {
            queriesForBindings.forEach((k, v) -> {
                String jdbcPrefix = "";
                try {
                    jdbcPrefix = Utils.getJDBCPrefix(v.getDbtype());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String finalJdbcPrefix = jdbcPrefix;
                // TODO: Support Roles when Trevas will be able to
                JDBCDataset jdbcDataset = new JDBCDataset(() -> {
                    try (
                            Connection connection = DriverManager.getConnection(
                                    finalJdbcPrefix + v.getUrl(),
                                    v.getUser(),
                                    v.getPassword())
                    ) {
                        Statement statement = connection.createStatement();
                        return statement.executeQuery(v.getQuery());
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
            throw new Exception(e);
        }
    }

    public ResponseEntity<EditVisualize> getJDBC(
            User user,
            QueriesForBindings queriesForBindings) throws SQLException {
        List<Map<String, Object>> structure = new ArrayList<>();
        List<List<Object>> points = new ArrayList<>();
        String jdbcPrefix = "";
        try {
            jdbcPrefix = Utils.getJDBCPrefix(queriesForBindings.getDbtype());
        } catch (Exception e) {
            e.printStackTrace();
        }
        try (
                Connection connection = DriverManager.getConnection(
                        jdbcPrefix + queriesForBindings.getUrl(),
                        queriesForBindings.getUser(),
                        queriesForBindings.getPassword())
        ) {
            try (
                    Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery(queriesForBindings.getQuery())
            ) {
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int columnCount = rsmd.getColumnCount();

                // Data
                while (resultSet.next()) {
                    List<Object> row = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        Object colVal = resultSet.getObject(i);
                        row.add(colVal);
                    }
                    points.add(row);
                }
            } catch (SQLException e) {
                throw new SQLException("JDBC connection error");
            }
        } catch (SQLException e) {
            throw new SQLException("JDBC connection error");
        }

        EditVisualize editVisualize = new EditVisualize();
        editVisualize.setDataStructure(structure);
        editVisualize.setDataPoints(points);

        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }

}
