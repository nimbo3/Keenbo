package in.nimbo.entity;

public class Anchor {
    private String href;
    private String content;

    public Anchor(String href, String content) {
        this.href = href;
        this.content = content;
    }

    public String getHref() {
        return href;
    }

    public String getContent() {
        return content;
    }
}
