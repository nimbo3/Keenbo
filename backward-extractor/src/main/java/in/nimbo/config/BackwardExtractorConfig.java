package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class BackwardExtractorConfig {
    private static final String CONFIG_NAME = "backward-extractor.properties";
    private String appName;
    private String nodesIP;
    private String esCreateIndex;
    private String esIndexName;
    private String esType;
    private String scanBatchSize;

    public static BackwardExtractorConfig load() {
        BackwardExtractorConfig backwardExtractorConfig = new BackwardExtractorConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            backwardExtractorConfig.setAppName(config.getString("app.name"));
            backwardExtractorConfig.setNodesIP(config.getString("nodes.ip"));
            backwardExtractorConfig.setEsCreateIndex(config.getString("es.index.auto.create"));
            backwardExtractorConfig.setEsIndexName(config.getString("es.index"));
            backwardExtractorConfig.setEsType(config.getString("es.type"));
            backwardExtractorConfig.setScanBatchSize(config.getString("hbase.scan.batch.size"));
            return backwardExtractorConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
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
