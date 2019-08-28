package in.nimbo.entity;

import java.io.Serializable;
import java.util.List;

public class PageNode implements Serializable {
    private String id;
    private Iterable<String> keywords;

    public PageNode() {}

    public PageNode(String id, Iterable<String> keywords) {
        this.id = id;
        this.keywords = keywords;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Iterable<String> getKeywords() {
        return keywords;
    }

    public void setKeywords(Iterable<String> keywords) {
        this.keywords = keywords;
    }
}
