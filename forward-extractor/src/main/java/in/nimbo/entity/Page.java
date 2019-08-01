package in.nimbo.entity;

import java.io.Serializable;
import java.util.List;

public class Page implements Serializable {
    private String id;
    private List<String> anchors;

    public Page() {}

    public Page(String id, List<String> anchors) {
        this.id = id;
        this.anchors = anchors;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getAnchors() {
        return anchors;
    }

    public void setAnchors(List<String> anchors) {
        this.anchors = anchors;
    }
}
