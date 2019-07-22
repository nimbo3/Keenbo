package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

public class ElasticConfig {
    private static final String CONFIG_NAME = "elastic-search.properties";
    private String indexName;
    private String host;
    private String type;
    private int port;

    public ElasticConfig() {
    }

    public static ElasticConfig load() {
        ElasticConfig elasticConfig = new ElasticConfig();
        Configurations configs = new Configurations();
        try {
            Configuration config = configs.properties(CONFIG_NAME);
            elasticConfig.setHost(config.getString("elastic.host"));
            elasticConfig.setPort(config.getInt("elastic.port"));
            elasticConfig.setIndexName(config.getString("elastic.index"));
            elasticConfig.setType(config.getString("elastic.type"));
            return elasticConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }


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
}
