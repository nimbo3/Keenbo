package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Node {
    @JsonProperty("id")
    private String domain;
    @JsonProperty("font")
    private Font font;
    @JsonIgnore
    private int pageCount;

    public Node(){}

    public Node(String domain, Font font, int pageCount) {
        this.domain = domain;
        this.font = font;
        this.pageCount = pageCount;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public Font getRank() {
        return font;
    }

    public void setRank(Font font) {
        this.font = font;
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Node node = (Node) o;

        return domain != null ? domain.equals(node.domain) : node.domain == null;
    }

    @Override
    public int hashCode() {
        return domain != null ? domain.hashCode() : 0;
    }
}
