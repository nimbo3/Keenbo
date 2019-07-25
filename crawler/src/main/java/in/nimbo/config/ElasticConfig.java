package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ElasticConfig {
    private static final String CONFIG_NAME = "elastic-search.properties";
    private String indexName;
    private String host;
    private String type;
    private int port;
    private int bulkActions;
    private long bulkSize;
    private String bulkSizeUnit;
    private int concurrentRequests;
    private String backoffDelaySeconds;
    private String backoffMaxRetry;

    public ElasticConfig() {
    }

    public static ElasticConfig load() {
        ElasticConfig elasticConfig = new ElasticConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            elasticConfig.setHost(config.getString("host"));
            elasticConfig.setPort(config.getInt("port"));
            elasticConfig.setIndexName(config.getString("index"));
            elasticConfig.setType(config.getString("type"));
            elasticConfig.setBulkActions(config.getInt("bulk.actions"));
            elasticConfig.setBulkSize(config.getLong("bulk.size"));
            elasticConfig.setBulkSizeUnit(config.getString("bulk.size.unit"));
            elasticConfig.setConcurrentRequests(config.getInt("concurrent.requests"));
            elasticConfig.setBackoffDelaySeconds(config.getString("backoff.delay.seconds"));
            elasticConfig.setBackoffMaxRetry(config.getString("backoff.max.retry"));
            return elasticConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
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

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getBulkActions() {
        return bulkActions;
    }

    public void setBulkActions(int bulkActions) {
        this.bulkActions = bulkActions;
    }

    public long getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(long bulkSize) {
        this.bulkSize = bulkSize;
    }

    public String getBulkSizeUnit() {
        return bulkSizeUnit;
    }

    public void setBulkSizeUnit(String bulkSizeUnit) {
        this.bulkSizeUnit = bulkSizeUnit;
    }

    public int getConcurrentRequests() {
        return concurrentRequests;
    }

    public void setConcurrentRequests(int concurrentRequests) {
        this.concurrentRequests = concurrentRequests;
    }

    public String getBackoffDelaySeconds() {
        return backoffDelaySeconds;
    }

    public void setBackoffDelaySeconds(String backoffDelaySeconds) {
        this.backoffDelaySeconds = backoffDelaySeconds;
    }

    public String getBackoffMaxRetry() {
        return backoffMaxRetry;
    }

    public void setBackoffMaxRetry(String backoffMaxRetry) {
        this.backoffMaxRetry = backoffMaxRetry;
    }
}
