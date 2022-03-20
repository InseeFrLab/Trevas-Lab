package fr.insee.vtl.lab.model;

import javax.script.Bindings;

public class SqlSpec {

    public String type;  // useful?
    public String url;
    public String user;
    public String password;
    public String query;
    public String dbtype;

    public String getQuery() {
        return query;
    }
}
