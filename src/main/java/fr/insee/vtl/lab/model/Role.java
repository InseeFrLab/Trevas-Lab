package fr.insee.vtl.lab.model;

import fr.insee.vtl.model.Dataset;

public class Role {

    private String name;
    private fr.insee.vtl.model.Dataset.Role role;

    public Role() {
    }

    public Role(String name, Dataset.Role role) {
        this.name = name;
        this.role = role;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Dataset.Role getRole() {
        return role;
    }

    public void setRole(Dataset.Role role) {
        this.role = role;
    }
}
