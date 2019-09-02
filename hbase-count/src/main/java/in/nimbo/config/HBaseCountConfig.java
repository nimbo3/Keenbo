package in.nimbo.config;

import in.nimbo.common.config.ElasticConfig;
import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class HBaseCountConfig {
    private static String CONFIG_NAME = "hbase-count.properties";
    private String tableName;
    private String appName;

    public static HBaseCountConfig load() {
        HBaseCountConfig hBaseCountConfig = new HBaseCountConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            hBaseCountConfig.setTableName(config.getString("table.name"));
            hBaseCountConfig.setAppName(config.getString("app.name"));
            return hBaseCountConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }
}
