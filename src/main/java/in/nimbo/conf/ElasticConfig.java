package in.nimbo.conf;

public class ElasticConfig {
    private String indexName;

    public ElasticConfig(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }
}
