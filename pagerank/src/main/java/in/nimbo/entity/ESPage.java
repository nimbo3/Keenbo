package in.nimbo.entity;

import java.io.Serializable;

public class ESPage implements Serializable {
    private String id;
    private double pagerank;

    public ESPage() {
    }

    public ESPage(String id, double pagerank) {
        this.id = id;
        this.pagerank = pagerank;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPagerank(double pagerank) {
        this.pagerank = pagerank;
    }

    public String getId() {
        return id;
    }

    public double getPagerank() {
        return pagerank;
    }
}
