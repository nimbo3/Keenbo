package in.nimbo.entity;

import java.io.Serializable;

public class Page implements Serializable {
    private String id;
    private Iterable<String> anchors;

    public Page() {
    }

    public Page(String id, Iterable<String> anchors) {
        this.id = id;
        this.anchors = anchors;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Iterable<String> getAnchors() {
        return anchors;
    }

    public void setAnchors(Iterable<String> anchors) {
        this.anchors = anchors;
    }
}
