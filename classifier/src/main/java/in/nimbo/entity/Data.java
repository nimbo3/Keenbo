package in.nimbo.entity;

public class Data {
    private double label;
    private String content;

    public Data() {}

    public Data(long label, String content) {
        this.label = (double) label;
        this.content = content;
    }

    public double getLabel() {
        return label;
    }

    public void setLabel(long label) {
        this.label = (double)label;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
