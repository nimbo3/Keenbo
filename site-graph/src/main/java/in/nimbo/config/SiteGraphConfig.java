package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class SiteGraphConfig {
    private static final String CONFIG_NAME = "site-graph.properties";
    private String resourceManager;
    private String appName;
    private String nodesIP;
    private String esCreateIndex;
    private String esIndexName;
    private String esType;
    private String scanBatchSize;

    public static SiteGraphConfig load() {
        SiteGraphConfig siteGraphConfig = new SiteGraphConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            siteGraphConfig.setResourceManager(config.getString("resource.manager"));
            siteGraphConfig.setAppName(config.getString("app.name"));
            siteGraphConfig.setNodesIP(config.getString("nodes.ip"));
            siteGraphConfig.setEsCreateIndex(config.getString("es.index.auto.create"));
            siteGraphConfig.setEsIndexName(config.getString("es.index"));
            siteGraphConfig.setEsType(config.getString("es.type"));
            siteGraphConfig.setScanBatchSize(config.getString("hbase.scan.batch.size"));
            return siteGraphConfig;
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

    public String getEsIndexName() {
        return esIndexName;
    }

    public void setEsIndexName(String esIndexName) {
        this.esIndexName = esIndexName;
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

    public String getScanBatchSize() {
        return this.scanBatchSize;
    }

    public void setScanBatchSize(String scanBatchSize) {
        this.scanBatchSize = scanBatchSize;
    }
}
