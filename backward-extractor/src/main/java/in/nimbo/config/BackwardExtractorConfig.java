package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class BackwardExtractorConfig {
    private static final String CONFIG_NAME = "backward-extractor.properties";
    private String appName;
    private String nodesIP;
    private String esIndexName;
    private String esType;

    public static BackwardExtractorConfig load() {
        BackwardExtractorConfig backwardExtractorConfig = new BackwardExtractorConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            backwardExtractorConfig.setAppName(config.getString("app.name"));
            backwardExtractorConfig.setNodesIP(config.getString("nodes.ip"));
            backwardExtractorConfig.setEsIndexName(config.getString("es.index"));
            backwardExtractorConfig.setEsType(config.getString("es.type"));
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

}
