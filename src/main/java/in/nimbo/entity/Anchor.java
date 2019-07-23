package in.nimbo.entity;

public class Anchor {
    private String href;
    private String anchor;

    public Anchor(String href, String anchor) {
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
