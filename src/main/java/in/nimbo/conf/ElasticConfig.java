package in.nimbo.conf;

public class ElasticConfig {
    private String indexName, host;
    private int port;

    public ElasticConfig(String indexName, String host, int port) {
        this.indexName = indexName;
        this.host = host;
        this.port = port;
    }

    public String getIndexName() {
        return indexName;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return host;
    }
}
