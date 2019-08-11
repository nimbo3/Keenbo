package in.nimbo.entity;

public class ESPage {
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
}
