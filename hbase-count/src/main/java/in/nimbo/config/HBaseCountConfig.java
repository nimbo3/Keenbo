package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class HBaseCountConfig {
    private static final String CONFIG_NAME = "hbase-count.properties";
    private String appName;
    private String scanBatchSize;

    public static HBaseCountConfig load() {
        HBaseCountConfig HBaseCountConfig = new HBaseCountConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            HBaseCountConfig.setAppName(config.getString("app.name"));
            HBaseCountConfig.setScanBatchSize(config.getString("hbase.scan.batch.size"));
            return HBaseCountConfig;
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
        return this.scanBatchSize;
    }

    public void setScanBatchSize(String scanBatchSize) {
        this.scanBatchSize = scanBatchSize;
    }
}
