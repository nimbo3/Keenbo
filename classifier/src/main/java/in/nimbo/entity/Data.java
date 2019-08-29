package in.nimbo.entity;

public class Data {
    private int label;
    private String content;

    public Data() {}

    public Data(int label, String content) {
        this.label = label;
        this.content = content;
    }

    public int getLabel() {
        return label;
    }

    public void setLabel(int label) {
        this.label = label;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
