package fr.insee.vtl.lab.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.insee.vtl.lab.configuration.security.UserProvider;
import fr.insee.vtl.lab.model.*;
import fr.insee.vtl.lab.service.InMemoryEngineV2;
import fr.insee.vtl.lab.service.SparkEngineV2;
import fr.insee.vtl.spark.SparkDataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static fr.insee.vtl.lab.utils.Utils.writeSparkDataset;

@RestController
@RequestMapping("/api/vtl/v2")
public class VtlLabControllerV2 {

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Map<UUID, JobV2> jobs = new HashMap<>();

    @Autowired
    private UserProvider userProvider;

    @Autowired
    private InMemoryEngineV2 inMemoryEngine;

    @Autowired
    private SparkEngineV2 sparkEngine;

    @Autowired
    private ObjectMapper objectMapper;

    @PostMapping("/in-memory")
    public Bindings executeInMemory(Authentication auth, @RequestBody BodyV2 body) {
        return inMemoryEngine.executeInMemory(userProvider.getUser(auth), body);
    }

    @PostMapping("/build-parquet")
    public String buildParquet(Authentication auth, @RequestBody ParquetPaths parquetPaths) {
        return sparkEngine.buildParquet(userProvider.getUser(auth), parquetPaths);
    }

    @PostMapping("/execute")
    public ResponseEntity<UUID> executeNew(
            Authentication auth,
            @RequestBody BodyV2 body,
            @RequestParam("mode") ExecutionMode mode,
            @RequestParam("type") ExecutionType type
    ) {
        JobV2 job;
        if (mode == ExecutionMode.MEMORY) {
            job = executeJob(body, () -> inMemoryEngine.executeInMemory(userProvider.getUser(auth), body));
        } else {
            switch (type) {
                case LOCAL:
                    job = executeJob(body, () -> sparkEngine.executeLocalSpark(userProvider.getUser(auth), body));
                    break;
                case CLUSTER_STATIC:
                    job = executeJob(body, () -> sparkEngine.executeSparkStatic(userProvider.getUser(auth), body));
                    break;
                case CLUSTER_KUBERNETES:
                    job = executeJob(body, () -> sparkEngine.executeSparkKube(userProvider.getUser(auth), body));
                    break;
                default:
                    throw new ResponseStatusException(
                            HttpStatus.BAD_REQUEST,
                            "Unsupported execution type: " + type
                    );
            }
        }
        jobs.put(job.id, job);
        return ResponseEntity.status(HttpStatus.CREATED)
                .header("Location", "/api/vtl/job/" + job.id)
                .body(job.id);
    }

    @GetMapping("/job/{jobId}")
    public JobV2 getJob(@PathVariable UUID jobId) {
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
    public JobV2 executeJob(BodyV2 body, VtlJob execution) {
        JobV2 job = new JobV2();
        executorService.submit(() -> {
            try {
                job.definition = body;
                for (String name : body.getToSave().keySet()) {
                    var output = new Output();
                    output.location = (String) body.getToSave().get(name);
                    job.outputs.put(name, output);
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
                        writeSparkDataset(objectMapper, spark, output.location, (SparkDataset) job.bindings.get(variableName));
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

    public enum ExecutionMode {
        MEMORY,
        SPARK
    }

    public enum ExecutionType {
        LOCAL,
        CLUSTER_STATIC,
        CLUSTER_KUBERNETES
    }

    @FunctionalInterface
    interface VtlJob {
        Bindings execute() throws ScriptException;
    }

}
