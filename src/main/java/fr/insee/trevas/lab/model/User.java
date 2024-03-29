package fr.insee.trevas.lab.model;

import java.util.List;

public class User {

    private String id;
    private String authToken;

    private List<String> groups;

    public User() {
    }

    public User(String id, String authToken) {
        this.id = id;
        this.authToken = authToken;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }
}
