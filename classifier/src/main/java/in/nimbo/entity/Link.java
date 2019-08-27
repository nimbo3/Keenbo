package in.nimbo.entity;

public class Link {
    private String url;
    private String label;
    private int level;

    public Link(String url, String label, int level) {
        this.url = url;
        this.label = label;
        this.level = level;
    }

    public String getUrl() {
        return url;
    }

    public String getLabel() {
        return label;
    }

    public int getLevel() {
        return level;
    }
}
