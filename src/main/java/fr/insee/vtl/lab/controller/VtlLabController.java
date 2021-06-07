package fr.insee.vtl.lab.controller;

import fr.insee.vtl.lab.configuration.security.UserProvider;
import fr.insee.vtl.lab.model.*;
import fr.insee.vtl.lab.service.InMemoryEngine;
import fr.insee.vtl.lab.service.SessionProvider;
import fr.insee.vtl.lab.service.SparkEngine;
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

@RestController
@RequestMapping("/api/vtl")
public class VtlLabController {

    @Autowired
    private UserProvider userProvider;

    @Autowired
    private InMemoryEngine inMemoryEngine;

    @Autowired
    private SparkEngine sparkEngine;

    @Autowired
    private SessionProvider sessionProvider;

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final Map<UUID, Job> jobs = new HashMap<>();

    @PostMapping("/in-memory")
    public Bindings executeInMemory(Authentication auth, @RequestBody Body body) {
        Map<String, Object> session = sessionProvider.getSession(auth);
        return inMemoryEngine.executeInMemory(userProvider.getUser(auth), body);
    }

    @PostMapping("/spark")
    public Bindings executeSpark(Authentication auth, @RequestBody Body body) throws ScriptException {
        return sparkEngine.executeLocalSpark(userProvider.getUser(auth), body);
    }

    @PostMapping("/spark-static")
    public Bindings executeSparkStatic(Authentication auth, @RequestBody Body body) throws ScriptException {
        return sparkEngine.executeSparkStatic(userProvider.getUser(auth), body);
    }

    @PostMapping("/spark-kube")
    public Bindings executeSparkKube(Authentication auth, @RequestBody Body body) throws ScriptException {
        return sparkEngine.executeSparkKube(userProvider.getUser(auth), body);
    }

    @PostMapping("/build-parquet")
    public String buildParquet(Authentication auth, @RequestBody ParquetPaths parquetPaths) {
        return sparkEngine.buildParquet(userProvider.getUser(auth), parquetPaths);
    }

    @FunctionalInterface
    interface VtlJob {
        Bindings execute() throws ScriptException;
    }

    @PostMapping("/spark-kube-new")
    public ResponseEntity<Void> executeNew(Authentication auth, @RequestBody Body body) {
        var job = executeJob(body, () -> {
            return sparkEngine.executeLocalSpark(userProvider.getUser(auth), body);
        });
        jobs.put(job.id, job);
        return ResponseEntity.status(HttpStatus.CREATED)
                .header("Location", "/api/vtl/job/" + job.id)
                .build();
    }

    @GetMapping("/job/{jobId}")
    public Job getJob(Authentication auth, @PathVariable UUID jobId) {
        if (!jobs.containsKey(jobId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND);
        }
        return jobs.get(jobId);
    }

    // TODO: Clean up the job map based on the date.
    public Job executeJob(Body body, VtlJob execution) {
        Job job = new Job();
        executorService.submit(() -> {
            try {
                job.definition = body;
                for (String name : body.getToSave().keySet()) {
                    var output = new Output();
                    output.location = (String) body.getToSave().get(name);
                }
                job.status = Status.RUNNING;
                Bindings bindings = execution.execute();
                for (String variableName : job.outputs.keySet()) {
                    final var output = job.outputs.get(variableName);
                    try {
                        output.status = Status.RUNNING;
                        Thread.sleep(30000);
                        // TODO: Refactor
                        //writeDataset((Dataset) bindings.get(variableName), output.location);
                        output.status = Status.DONE;
                    } catch (Exception ex) {
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

}
