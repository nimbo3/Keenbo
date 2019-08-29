package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Page {
    @JsonProperty("link")
    private String link;
    @JsonProperty("title")
    private String title;
    @JsonProperty("content")
    private String content;
    @JsonIgnore
    private int label;

    public Page() {
    }

    public Page(String link, String title, String content) {
        this.title = title;
        this.content = content;
        this.link = link;
    }

    public int getLabel() {
        return label;
    }

    public void setLabel(long label) {
        this.label = (int)label;
    }

    public String getTitle() {
        return title;
    }

    public String getLink() {
        return link;
    }

    public String getContent() {
        return content;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
