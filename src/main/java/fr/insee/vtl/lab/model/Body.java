package fr.insee.vtl.lab.model;

import javax.script.Bindings;

public class Body {

    private String vtlScript;
    private Bindings bindings;
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

    public Bindings getToSave() {
        return toSave;
    }

    public void setToSave(Bindings toSave) {
        this.toSave = toSave;
    }
}
