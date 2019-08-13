package in.nimbo.entity;

public class Edge {
    private String src;
    private String dst;
    private int numOfAnchors;

    public Edge() {}

    public Edge(String src, String dst) {
        this.src = src;
        this.dst = dst;
        this.numOfAnchors = 1;
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

    public int getNumOfAnchors() {
        return numOfAnchors;
    }

    public void setNumOfAnchors(int numOfAnchors) {
        this.numOfAnchors = numOfAnchors;
    }
}
