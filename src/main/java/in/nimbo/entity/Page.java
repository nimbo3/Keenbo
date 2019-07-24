package in.nimbo.entity;

import in.nimbo.utility.LinkUtility;

import java.net.MalformedURLException;
import java.util.List;

public class Page {
    private String link;
    private String title;
    private String contentWithTags;
    private String contentWithoutTags;
    private List<Anchor> anchors;
    private List<Meta> metas;
    private double rank;
    private String reversedLink;

    public Page() {
    }

    public Page(String link, String title, String contentWithTags, String contentWithoutTags, List<Anchor> anchors, List<Meta> metas,
                double rank) {
        this.title = title;
        this.contentWithTags = contentWithTags;
        this.contentWithoutTags = contentWithoutTags;
        this.anchors = anchors;
        this.metas = metas;
        this.link = link;
        this.rank = rank;
    }

    public String getTitle() {
        return title;
    }

    public String getReversedLink() {
        if (reversedLink == null)
            try {
                return reversedLink = LinkUtility.reverseLink(link);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        return reversedLink;
    }

    public String getLink() {
        return link;
    }

    public double getRank() {
        return rank;
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

    public void setLink(String link) {
        this.link = link;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setContentWithTags(String contentWithTags) {
        this.contentWithTags = contentWithTags;
    }

    public void setContentWithoutTags(String contentWithoutTags) {
        this.contentWithoutTags = contentWithoutTags;
    }

    public void setAnchors(List<Anchor> anchors) {
        this.anchors = anchors;
    }

    public void setMetas(List<Meta> metas) {
        this.metas = metas;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public void setReversedLink(String reversedLink) {
        this.reversedLink = reversedLink;
    }
}
