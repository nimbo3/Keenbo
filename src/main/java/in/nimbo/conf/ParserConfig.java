package in.nimbo.conf;

public class ParserConfig {
    private int timeout;

    public ParserConfig(int timeout) {
        this.timeout = timeout;
    }

    public int getTimeout() {
        return timeout;
    }
}
