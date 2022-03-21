package fr.insee.vtl.lab.model;

import javax.script.Bindings;
import java.util.Map;

public class BodyV2 {

    private String vtlScript;
    private Bindings bindings;
    private Map<String, QueriesForBindings> queriesForBindings;
    private Bindings toSave;

    public String getVtlScript() {
        return vtlScript;
    }

    public void setVtlScript(String vtlScript) {
        this.vtlScript = vtlScript;
    }

    public Bindings getBindings() {
        return bindings;
    }

    public void setBindings(Bindings bindings) {
        this.bindings = bindings;
    }

    public Map<String, QueriesForBindings> getQueriesForBindings() {
        return queriesForBindings;
    }

    public void setQueriesForBindings(Map<String, QueriesForBindings> queriesForBindings) {
        this.queriesForBindings = queriesForBindings;
    }

    public Bindings getToSave() {
        return toSave;
    }

    public void setToSave(Bindings toSave) {
        this.toSave = toSave;
    }
}
