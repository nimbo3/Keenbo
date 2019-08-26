package in.nimbo.entity;

public class Edge {
    private String src;
    private String dst;

    public Edge() {}

    public Edge(String src, String dst) {
        this.src = src;
        this.dst = dst;
    }

    public String getSrc() {
        return src;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public String getDst() {
        return dst;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }
}
