package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class SiteGraphConfig {
    private static final String CONFIG_NAME = "site-graph.properties";
    private String appName;
    private String scanBatchSize;

    public static SiteGraphConfig load() {
        SiteGraphConfig appConfig = new SiteGraphConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            appConfig.setAppName(config.getString("app.name"));
            appConfig.setScanBatchSize(config.getString("hbase.scan.batch.size"));
            return appConfig;
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

    public String getScanBatchSize() {
        return scanBatchSize;
    }

    public void setScanBatchSize(String scanBatchSize) {
        this.scanBatchSize = scanBatchSize;
    }
}
