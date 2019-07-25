package in.nimbo.entity;

public class Meta {
    private String key;
    private String content;

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
}
