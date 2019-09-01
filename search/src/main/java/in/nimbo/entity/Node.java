package in.nimbo.entity;

public class Node {
    private String id;
    private Font font;
    private int pageCount;

    public Node() {
    }

    public Node(String id, Font font, int pageCount) {
        this.id = id;
        this.font = font;
        this.pageCount = pageCount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Font getFont() {
        return font;
    }

    public void setFont(Font font) {
        this.font = font;
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }
}
