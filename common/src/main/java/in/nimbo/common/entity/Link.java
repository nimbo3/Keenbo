package in.nimbo.common.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Link {
    @JsonProperty("url")
    private String url;
    @JsonProperty("label")
    private String label;
    @JsonProperty("level")
    private int level;

    public Link(String url, String label, int level) {
        this.url = url;
        this.label = label;
        this.level = level;
    }

    public Link(){}

    public void setUrl(String url) {
        this.url = url;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public String getUrl() {
        return url;
    }

    public String getLabel() {
        return label;
    }

    public int getLevel() {
        return level;
    }
}
