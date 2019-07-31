package in.nimbo.entity;

public class RedisNodeStatus {
    private String node;
    private long value;

    public RedisNodeStatus(String node, long value) {
        this.node = node;
        this.value = value;
    }

    public String getNode() {
        return node;
    }

    public long getValue() {
        return value;
    }
}
