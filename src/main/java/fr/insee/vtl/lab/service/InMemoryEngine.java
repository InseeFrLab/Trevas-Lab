package fr.insee.vtl.lab.service;

import fr.insee.vtl.jdbc.JDBCDataset;
import fr.insee.vtl.lab.model.Body;
import fr.insee.vtl.lab.model.EditVisualize;
import fr.insee.vtl.lab.model.QueriesForBindings;
import fr.insee.vtl.lab.model.User;
import fr.insee.vtl.lab.utils.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static fr.insee.vtl.lab.utils.Utils.getJDBCPrefix;

@Service
public class InMemoryEngine {

    private static final Logger logger = LogManager.getLogger(InMemoryEngine.class);

    public Bindings executeInMemory(User user, Body body) throws SQLException {
        String script = body.getVtlScript();
        Bindings bindings = body.getBindings();
        Map<String, QueriesForBindings> queriesForBindings = body.getQueriesForBindings();

        if (queriesForBindings != null) {
            queriesForBindings.forEach((k, v) -> {
                String jdbcPrefix = "";
                try {
                    jdbcPrefix = getJDBCPrefix(v.getDbtype());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                String finalJdbcPrefix = jdbcPrefix;
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
        }
        return bindings;
    }

    public ResponseEntity<EditVisualize> getJDBC(
            User user,
            QueriesForBindings queriesForBindings) {
        List<Map<String, Object>> structure = new ArrayList<>();
        List<List<Object>> points = new ArrayList<>();
        String jdbcPrefix = "";
        try {
            jdbcPrefix = getJDBCPrefix(queriesForBindings.getDbtype());
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
                // Structure
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int columnCount = rsmd.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    Map<String, Object> row = new HashMap<>();
                    String colName = rsmd.getColumnName(i);
                    row.put("name", colName);
                    String colType = JDBCType.valueOf(rsmd.getColumnType(i)).getName();
                    row.put("type", colType);
                    structure.add(row);
                }

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
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        EditVisualize editVisualize = new EditVisualize();
        editVisualize.setDataStructure(structure);
        editVisualize.setDataPoints(points);

        return ResponseEntity.status(HttpStatus.OK)
                .body(editVisualize);
    }

}
