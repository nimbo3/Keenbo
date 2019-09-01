package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Category {
    @JsonProperty("name")
    private String name;
    @JsonProperty("sites")
    private List<String> sites;

    public String getName() {
        return name;
    }

    public List<String> getSites() {
        return sites;
    }
}
