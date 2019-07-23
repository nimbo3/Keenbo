package in.nimbo.entity;

import java.util.List;

public class Page {
    private String contentWithTags;
    private String contentWithOutTags;
    private List<Link> links;
    private List<Meta> metas;

    public Page(String contentWithTags, String contentWithOutTags, List<Link> links, List<Meta> metas) {
        this.contentWithTags = contentWithTags;
        this.contentWithOutTags = contentWithOutTags;
        this.links = links;
        this.metas = metas;
    }

    public String getContentWithTags() {
        return contentWithTags;
    }

    public String getContentWithOutTags() {
        return contentWithOutTags;
    }

    public List<Link> getLinks() {
        return links;
    }

    public List<Meta> getMetas() {
        return metas;
    }
}
