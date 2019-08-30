package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class SiteGraphConfig {
    public enum MODE {EXTRACTOR, GRAPH}

    private static final String CONFIG_NAME = "site-graph.properties";
    private String appName;
    private MODE appMode;

    public static SiteGraphConfig load() {
        SiteGraphConfig appConfig = new SiteGraphConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            appConfig.setAppName(config.getString("app.name"));
            String appMode = config.getString("app.mode");
            if (appMode.equals("extractor")) {
                appConfig.setAppMode(MODE.EXTRACTOR);
            } else if (appMode.equals("graph")) {
                appConfig.setAppMode(MODE.GRAPH);
            } else {
                throw new ConfigurationException(CONFIG_NAME + ": app mode is illegal");
            }
            return appConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public MODE getAppMode() {
        return appMode;
    }

    public void setAppMode(MODE appMode) {
        this.appMode = appMode;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

}
