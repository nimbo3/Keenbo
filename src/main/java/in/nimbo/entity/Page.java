package in.nimbo.entity;

import java.util.List;

public class Page {
    private String content;
    private List<String> links;

    public Page(String content, List<String> links) {
        this.content = content;
        this.links = links;
    }

    public String getContent() {
        return content;
    }

    public List<String> getLinks() {
        return links;
    }
}
