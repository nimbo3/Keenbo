package in.nimbo.entity;

import java.io.Serializable;
import java.util.List;

public class Page implements Serializable {
    private String id;
    private List<String> anchors;
    private long anchorsSize;

    public Page() {
    }

    public Page(String id, List<String> anchors, long anchorsSize) {
        this.id = id;
        this.anchors = anchors;
        this.anchorsSize = anchorsSize;
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

    public void setLength(int anchorsLength) {
        this.anchorsSize = anchorsLength;
    }

    public long getAnchorsSize() {
        return anchorsSize;
    }

    public void setAnchorsSize(long anchorsSize) {
        this.anchorsSize = anchorsSize;
    }
}
