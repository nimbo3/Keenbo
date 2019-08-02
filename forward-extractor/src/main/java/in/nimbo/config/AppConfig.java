package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class AppConfig {
    private static final String CONFIG_NAME = "app-config.properties";
    private String resourceManager;
    private String appName;
    private String nodesIP;
    private String esCreateIndex;
    private String hbaseTable;
    private String hbaseColumnFamily;
    private String esIndexName;
    private String esTableName;

    public static AppConfig load() {
        AppConfig appConfig = new AppConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            appConfig.setResourceManager(config.getString("resource.manager"));
            appConfig.setAppName(config.getString("app.name"));
            appConfig.setNodesIP(config.getString("nodes.ip"));
            appConfig.setEsCreateIndex(config.getString("es.index.auto.create"));
            appConfig.setEsIndexName(config.getString("es.index.name"));
            appConfig.setEsTableName(config.getString("es.tale.name"));
            appConfig.setHbaseTable(config.getString("hbase.table"));
            appConfig.setHbaseColumnFamily(config.getString("hbase.column.family"));
            return appConfig;
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
