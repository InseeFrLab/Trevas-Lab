package fr.insee.vtl.lab.model;

import javax.script.Bindings;

public class SqlSpec {

    private String type;  // useful?
    private String url;
    private String user;
    private String password;
    private String query;
    private String dbtype;

    public String getQuery() {
        return query;
    }
}
