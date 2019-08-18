package in.nimbo.common.entity;

import java.util.Objects;

public class Meta {
    private String key;
    private String content;

    public Meta(){}

    public Meta(String key, String content) {
        this.key = key;
        this.content = content;
    }

    public String getKey() {
        return key;
    }

    public String getContent() {
        return content;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Meta meta = (Meta) o;
        return Objects.equals(key, meta.key) &&
                Objects.equals(content, meta.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, content);
    }
}
