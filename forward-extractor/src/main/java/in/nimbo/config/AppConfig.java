package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class AppConfig {
    private static final String CONFIG_NAME = "app-config.properties";
    private String resourceManager;
    private String appName;
    private String nodesIP;

    public static AppConfig load() {
        AppConfig appConfig = new AppConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            appConfig.setResourceManager(config.getString("resource.manager"));
            appConfig.setAppName(config.getString("app.name"));
            appConfig.setMasterIP(config.getString("nodes.ip"));
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

    public String getMasterIP() {
        return nodesIP;
    }

    public void setMasterIP(String masterIP) {
        this.nodesIP = masterIP;
    }
}
