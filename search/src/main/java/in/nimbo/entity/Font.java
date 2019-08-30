package in.nimbo.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Font {
    @JsonProperty("size")
    private double size;

    public Font() {}

    public Font(double size) {
        this.size = size;
    }

    public double getSize() {
        return size;
    }

    public void setSize(double size) {
        this.size = size;
    }
}
