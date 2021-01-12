package fr.insee.vtl.lab.controller;

import fr.insee.vtl.lab.model.Body;
import fr.insee.vtl.lab.service.InMemoryEngine;
import fr.insee.vtl.lab.service.SparkEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.script.Bindings;
import javax.script.ScriptException;

@RestController
@CrossOrigin()
@RequestMapping("/api/vtl")
public class VtlLabController {

    @Autowired
    private InMemoryEngine inMemoryEngine;

    @Autowired
    private SparkEngine sparkEngine;

    @GetMapping
    public String helloVTL() {
        return "Hello VTL";
    }

    @PostMapping("/in-memory")
    public Bindings executeInMemory(@RequestBody Body body) {
        return inMemoryEngine.executeInMemory(body);
    }

    @PostMapping("/spark")
    public Bindings executeSpark(@RequestBody Body body) throws ScriptException {
        return sparkEngine.executeLocalSpark(body);
    }

    @PostMapping("/spark-cluster")
    public Bindings executeSparkCluster(@RequestBody Body body) throws ScriptException {
        return sparkEngine.executeSparkCluster(body);
    }

}
