package fr.insee.trevas.lab.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.script.Bindings;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class Job {
    public UUID id = UUID.randomUUID();
    public Body definition;
    public Status status = Status.READY;
    public Map<String, Output> outputs = new HashMap<>();
    public Exception error;

    @JsonIgnore
    public Bindings bindings;


    public Job() {
    }
}
