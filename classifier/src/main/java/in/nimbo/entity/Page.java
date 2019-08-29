package in.nimbo.entity;

public class Page {
    private String content;
    private String link;
    private int label;

    public Page(String content, String link, int label) {
        this.content = content;
        this.link = link;
        this.label = label;
    }

    public String getContent() {
        return content;
    }

    public String getLink() {
        return link;
    }

    public int getLabel() {
        return label;
    }
}
