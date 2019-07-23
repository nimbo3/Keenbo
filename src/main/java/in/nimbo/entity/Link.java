package in.nimbo.entity;

public class Link {
    private String href;
    private String anchor;

    public Link(String href, String anchor) {
        this.href = href;
        this.anchor = anchor;
    }

    public String getHref() {
        return href;
    }

    public String getAnchor() {
        return anchor;
    }
}
