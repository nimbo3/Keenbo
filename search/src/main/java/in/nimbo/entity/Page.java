package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Page {
    @JsonProperty("link")
    private String link;
    @JsonProperty("title")
    private String title;
    @JsonIgnore
    private String contentWithTags;
    @JsonIgnore
    private String contentWithoutTags;
    @JsonIgnore
    private double rank;

    public Page() {
    }

    public Page(String link, String title, String contentWithTags, String contentWithoutTags, double rank) {
        this.title = title;
        this.contentWithTags = contentWithTags;
        this.contentWithoutTags = contentWithoutTags;
        this.link = link;
        this.rank = rank;
    }

    public String getTitle() {
        return title;
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

    public void setRank(double rank) {
        this.rank = rank;
    }
}
