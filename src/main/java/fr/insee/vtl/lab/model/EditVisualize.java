package fr.insee.vtl.lab.model;

import java.util.List;
import java.util.Map;

public class EditVisualize {

    private List<Map<String, Object>> dataStructure;
    private List<List<Object>> dataPoints;

    public List<Map<String, Object>> getDataStructure() {
        return dataStructure;
    }

    public void setDataStructure(List<Map<String, Object>> dataStructure) {
        this.dataStructure = dataStructure;
    }

    public List<List<Object>> getDataPoints() {
        return dataPoints;
    }

    public void setDataPoints(List<List<Object>> dataPoints) {
        this.dataPoints = dataPoints;
    }
}
