package fr.insee.vtl.lab.model;

import java.util.Map;

public class ToSave {

    private Map<String, S3ForBindings> s3ForBindings;
    private Map<String, QueriesForBindingsToSave> jdbcForBindingsToSave;

    public Map<String, S3ForBindings> getS3ForBindings() {
        return s3ForBindings;
    }

    public void setS3ForBindings(Map<String, S3ForBindings> s3ForBindings) {
        this.s3ForBindings = s3ForBindings;
    }

    public Map<String, QueriesForBindingsToSave> getJdbcForBindingsToSave() {
        return jdbcForBindingsToSave;
    }

    public void setJdbcForBindingsToSave(Map<String, QueriesForBindingsToSave> jdbcForBindingsToSave) {
        this.jdbcForBindingsToSave = jdbcForBindingsToSave;
    }
}
