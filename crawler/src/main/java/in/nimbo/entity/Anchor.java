package in.nimbo.entity;

import java.util.Objects;

public class Anchor {
    private String href;
    private String content;

    public Anchor(String href, String content) {
        this.href = href;
        this.content = content;
    }

    public String getHref() {
        return href;
    }

    public String getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Anchor anchor = (Anchor) o;
        return Objects.equals(href, anchor.href) &&
                Objects.equals(content, anchor.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(href, content);
    }
}
