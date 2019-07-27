package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Page {
    @JsonProperty("link")
    private String link;
    @JsonProperty("title")
    private String title;
    @JsonIgnore
    private String contentWithoutTags;

    public Page() {
    }

    public Page(String link, String title, String contentWithoutTags) {
        this.title = title;
        this.contentWithoutTags = contentWithoutTags;
        this.link = link;
    }

    public String getTitle() {
        return title;
    }


    public String getLink() {
        return link;
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

    public void setContentWithoutTags(String contentWithoutTags) {
        this.contentWithoutTags = contentWithoutTags;
    }
}
