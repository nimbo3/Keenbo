package in.nimbo.entity;

public class Data {
    private double label;
    private String content;

    public Data() {}

    public Data(double label, String content) {
        this.label = label;
        this.content = content;
    }

    public double getLabel() {
        return label;
    }

    public void setLabel(double label) {
        this.label = label;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
