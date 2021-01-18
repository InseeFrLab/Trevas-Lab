package fr.insee.vtl.lab.controller;

import fr.insee.vtl.lab.configuration.security.UserProvider;
import fr.insee.vtl.lab.model.Body;
import fr.insee.vtl.lab.service.InMemoryEngine;
import fr.insee.vtl.lab.service.SparkEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.script.Bindings;
import javax.script.ScriptException;

@RestController
@RequestMapping("/api/vtl")
public class VtlLabController {

    @Autowired
    private UserProvider userProvider;

    @Autowired
    private InMemoryEngine inMemoryEngine;

    @Autowired
    private SparkEngine sparkEngine;

    @GetMapping
    public String helloVTL() {
        return "Hello VTL";
    }

    @PostMapping("/in-memory")
    public Bindings executeInMemory(Authentication auth, @RequestBody Body body) {
        return inMemoryEngine.executeInMemory(userProvider.getUser(auth), body);
    }

    @PostMapping("/spark")
    public Bindings executeSpark(Authentication auth, @RequestBody Body body) throws ScriptException {
        return sparkEngine.executeLocalSpark(userProvider.getUser(auth), body);
    }

    @PostMapping("/spark-cluster")
    public Bindings executeSparkCluster(Authentication auth, @RequestBody Body body) throws ScriptException {
        return sparkEngine.executeSparkCluster(userProvider.getUser(auth), body);
    }

}
