package in.nimbo.common.entity;

import in.nimbo.common.utility.LinkUtility;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Set;

public class Page {
    private String link;
    private String title;
    private String content;
    private Set<Anchor> anchors;
    private List<Meta> metas;
    private double rank;
    private String reversedLink;
    private long linkDepth;

    public Page() {
    }

    public Page(String link, String title, String content, Set<Anchor> anchors, List<Meta> metas,
                double rank) throws MalformedURLException {
        this.title = title;
        this.content = content;
        this.anchors = anchors;
        this.metas = metas;
        this.link = link;
        this.rank = rank;
        this.reversedLink = LinkUtility.reverseLink(link);
        this.linkDepth = LinkUtility.depth(link);
    }

    public String getTitle() {
        return title;
    }

    public String getReversedLink() {
        return reversedLink;
    }

    public String getLink() {
        return link;
    }

    public double getRank() {
        return rank;
    }

    public String getContent() {
        return content;
    }

    public Set<Anchor> getAnchors() {
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

    public void setContent(String content) {
        this.content = content;
    }

    public void setAnchors(Set<Anchor> anchors) {
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

    public long getLinkDepth() {
        return linkDepth;
    }
}
