package in.nimbo.entity;

import java.io.Serializable;
import java.util.Date;

public class Page implements Serializable {
    private String id;
    private Iterable<String> anchors;
    private long anchorsSize;
    private long timestamp;

    public Page() {
    }

    public Page(String id, Iterable<String> anchors, long anchorsSize) {
        this.id = id;
        this.anchors = anchors;
        this.anchorsSize = anchorsSize;
        this.timestamp = new Date().getTime();
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

    public long getLength() {
        return anchorsSize;
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

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
