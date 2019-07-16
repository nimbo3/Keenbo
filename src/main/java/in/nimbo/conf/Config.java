package in.nimbo.conf;

public class Config {
    private int timeout;
    private int maximumSize;
    private int expireCacheTime;
    private String linksTopic;

    public Config(int timeout, int maximumSize, int expireCacheTime, String linksTopic) {
        this.timeout = timeout;
        this.maximumSize = maximumSize;
        this.expireCacheTime = expireCacheTime;
        this.linksTopic = linksTopic;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getMaximumSize() {
        return maximumSize;
    }

    public int getExpireCacheTime() {
        return expireCacheTime;
    }

    public String getLinksTopic() {
        return linksTopic;
    }

    public void setLinksTopic(String linksTopic) {
        this.linksTopic = linksTopic;
    }
}
