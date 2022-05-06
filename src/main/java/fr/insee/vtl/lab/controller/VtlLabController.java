package fr.insee.vtl.lab.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.configuration.security.UserProvider;
import fr.insee.vtl.lab.model.*;
import fr.insee.vtl.lab.service.InMemoryEngine;
import fr.insee.vtl.lab.service.SparkEngine;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
@RequestMapping("/api/vtl")
public class VtlLabController {

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Map<UUID, Job> jobs = new HashMap<>();

    @Autowired
    private UserProvider userProvider;

    @Autowired
    private InMemoryEngine inMemoryEngine;

    @Autowired
    private SparkEngine sparkEngine;

    @Autowired
    private ObjectMapper objectMapper;

    @PostMapping("/connect")
    public ResponseEntity<EditVisualize> getDataFromConnector(
            Authentication auth,
            @RequestBody Body body,
            @RequestParam("mode") ExecutionMode mode,
            @RequestParam("connectorType") ConnectorType connectorType,
            @RequestParam("type") ExecutionType type
    ) throws Exception {
        if (mode == ExecutionMode.MEMORY) {
            if (connectorType == ConnectorType.JDBC)
                return inMemoryEngine.getJDBC(userProvider.getUser(auth), body.getQueriesForBindings().get("config"));
            else throw new Exception("Unknow connector type: " + mode);
        } else if (mode == ExecutionMode.SPARK) {
            if (connectorType == ConnectorType.JDBC)
                return sparkEngine.getJDBC(userProvider.getUser(auth), body.getQueriesForBindings().get("config"), type);
            else if (connectorType == ConnectorType.S3)
                return sparkEngine.getS3(userProvider.getUser(auth), body.getS3ForBindings().get("config"), type);
            else throw new Exception("Unknow connector type: " + mode);
        } else throw new Exception("Unknow mode: " + mode);
    }

    @PostMapping("/execute")
    public ResponseEntity<UUID> executeNew(
            Authentication auth,
            @RequestBody Body body,
            @RequestParam("mode") ExecutionMode mode,
            @RequestParam("type") ExecutionType type
    ) throws Exception {
        Job job;
        if (mode == ExecutionMode.MEMORY) {
            job = executeJob(body, () -> {
                try {
                    return inMemoryEngine.executeInMemory(userProvider.getUser(auth), body);
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new ResponseStatusException(
                            HttpStatus.INTERNAL_SERVER_ERROR,
                            "SQLException error: " + type
                    );
                }
            });
        } else if (mode == ExecutionMode.SPARK) {
            switch (type) {
                case LOCAL:
                case CLUSTER_STATIC:
                case CLUSTER_KUBERNETES:
                    job = executeJob(body, () -> {
                        try {
                            return sparkEngine.executeSpark(userProvider.getUser(auth), body, type);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    });
                    break;
                default:
                    throw new ResponseStatusException(
                            HttpStatus.BAD_REQUEST,
                            "Unsupported execution type: " + type
                    );
            }
        } else throw new Exception("Unknow mode:" + mode);
        jobs.put(job.id, job);
        return ResponseEntity.status(HttpStatus.CREATED)
                .header("Location", "/api/vtl/job/" + job.id)
                .body(job.id);
    }

    @GetMapping("/job/{jobId}")
    public Job getJob(@PathVariable UUID jobId) {
        if (!jobs.containsKey(jobId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return jobs.get(jobId);
    }

    @GetMapping("/job/{jobId}/bindings")
    public Bindings getJobBinding(@PathVariable UUID jobId) {
        if (!jobs.containsKey(jobId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return jobs.get(jobId).bindings;
    }

    // TODO: Move to service.
    // TODO: Clean up the job map based on the date.
    // TODO: Refactor to use the ScriptEngine inside the user session.
    public Job executeJob(Body body, VtlJob execution) {
        Job job = new Job();
        executorService.submit(() -> {
            try {
                job.definition = body;
                Map<String, S3ForBindings> s3ToSave = body.getToSave().getS3ForBindings();
                if (s3ToSave != null) {
                    s3ToSave.forEach((k, v) -> {
                        var output = new Output();
                        output.location = k;
                        job.outputs.put(k, output);
                    });
                }
                Map<String, QueriesForBindingsToSave> jdbcToSave = body.getToSave().getJdbcForBindingsToSave();
                if (jdbcToSave != null) {
                    jdbcToSave.forEach((k, v) -> {
                        var output = new Output();
                        output.location = k;
                        job.outputs.put(k, output);
                    });
                }
                job.status = Status.RUNNING;
                job.bindings = execution.execute();
                for (String variableName : job.outputs.keySet()) {
                    final var output = job.outputs.get(variableName);
                    try {
                        output.status = Status.RUNNING;
                        SparkSession.Builder sparkBuilder = SparkSession.builder()
                                .appName("vtl-lab")
                                .master("local");
                        SparkSession spark = sparkBuilder.getOrCreate();
//                        writeSparkDataset(objectMapper, spark, output.location, (SparkDataset) job.bindings.get(variableName));
                        output.status = Status.DONE;
                    } catch (Exception ex) {
                        job.status = Status.FAILED;
                        output.status = Status.FAILED;
                        output.error = ex;
                    }
                }
                job.status = Status.DONE;
            } catch (Exception e) {
                job.status = Status.FAILED;
                job.error = e;
            }
        });
        return job;
    }

    @FunctionalInterface
    interface VtlJob {
        Bindings execute() throws ScriptException;
    }

}
