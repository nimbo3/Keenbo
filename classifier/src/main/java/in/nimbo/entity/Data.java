package in.nimbo.entity;

public class Data {
    private long label;
    private String content;

    public Data() {}

    public Data(long label, String content) {
        this.label = label;
        this.content = content;
    }

    public long getLabel() {
        return label;
    }

    public void setLabel(long label) {
        this.label = label;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
