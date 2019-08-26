package in.nimbo.entity;

public class Link {
    private String url;
    private String label;

    public Link(String url, String label) {
        this.url = url;
        this.label = label;
    }

    public String getUrl() {
        return url;
    }

    public String getLabel() {
        return label;
    }
}
