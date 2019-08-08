package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class PageRankConfig {
    private static final String CONFIG_NAME = "app-config.properties";
    private String resourceManager;
    private String appName;
    private String nodesIP;
    private String esCreateIndex;
    private String hbaseTable;
    private String hbaseColumnFamily;
    private String esIndexName;
    private String esTableName;

    public static PageRankConfig load() {
        PageRankConfig pageRankConfig = new PageRankConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            pageRankConfig.setResourceManager(config.getString("resource.manager"));
            pageRankConfig.setAppName(config.getString("app.name"));
            pageRankConfig.setNodesIP(config.getString("nodes.ip"));
            pageRankConfig.setEsCreateIndex(config.getString("es.index.auto.create"));
            pageRankConfig.setEsIndexName(config.getString("es.index"));
            pageRankConfig.setEsTableName(config.getString("es.type"));
            pageRankConfig.setHbaseTable(config.getString("hBase.table"));
            pageRankConfig.setHbaseColumnFamily(config.getString("hBase.column.family"));
            return pageRankConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getResourceManager() {
        return resourceManager;
    }

    public void setResourceManager(String resourceManager) {
        this.resourceManager = resourceManager;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getNodesIP() {
        return nodesIP;
    }

    public void setNodesIP(String masterIP) {
        this.nodesIP = masterIP;
    }

    public String getEsCreateIndex() {
        return esCreateIndex;
    }

    public void setEsCreateIndex(String esCreateIndex) {
        this.esCreateIndex = esCreateIndex;
    }

    public String getHbaseTable() {
        return hbaseTable;
    }

    public void setHbaseTable(String hbaseTable) {
        this.hbaseTable = hbaseTable;
    }

    public String getHbaseColumnFamily() {
        return hbaseColumnFamily;
    }

    public void setHbaseColumnFamily(String hbaseColumnFamily) {
        this.hbaseColumnFamily = hbaseColumnFamily;
    }

    public String getEsIndexName() {
        return esIndexName;
    }

    public void setEsIndexName(String esIndexName) {
        this.esIndexName = esIndexName;
    }

    public String getEsTableName() {
        return esTableName;
    }

    public void setEsTableName(String esTableName) {
        this.esTableName = esTableName;
    }
}
