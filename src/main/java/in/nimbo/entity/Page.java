package in.nimbo.entity;

import in.nimbo.utility.LinkUtility;

import java.util.List;

public class Page {
    private String link;
    private String title;
    private String contentWithTags;
    private String contentWithoutTags;
    private List<Anchor> anchors;
    private List<Meta> metas;
    private double pageRate;
    private String reversedLink;

    public Page(String link, String title, String contentWithTags, String contentWithoutTags, List<Anchor> anchors, List<Meta> metas,
                double pageRate) {
        this.title = title;
        this.contentWithTags = contentWithTags;
        this.contentWithoutTags = contentWithoutTags;
        this.anchors = anchors;
        this.metas = metas;
        this.link = link;
        this.pageRate = pageRate;
    }

    public String getTitle() {
        return title;
    }

    public String getReversedLink() {
        if (reversedLink == null)
            return reversedLink = LinkUtility.reverseLink(link);
        return reversedLink;
    }

    public String getLink() {
        return link;
    }

    public double getPageRate() {
        return pageRate;
    }

    public String getContentWithTags() {
        return contentWithTags;
    }

    public String getContentWithoutTags() {
        return contentWithoutTags;
    }

    public List<Anchor> getAnchors() {
        return anchors;
    }

    public List<Meta> getMetas() {
        return metas;
    }
}
