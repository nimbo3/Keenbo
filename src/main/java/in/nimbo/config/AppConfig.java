package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class AppConfig {
    private static final String CONFIG_NAME = "app-config.properties";
    private int caffeineMaxSize;
    private int caffeineExpireTime;
    private int jsoupTimeout;

    public static AppConfig load() {
        AppConfig appConfig = new AppConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            appConfig.setCaffeineMaxSize(config.getInt("caffeine.max.size"));
            appConfig.setCaffeineExpireTime(config.getInt("caffeine.expire.time"));
            appConfig.setJsoupTimeout(config.getInt("jsoup.timeout"));
            return appConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public int getCaffeineMaxSize() {
        return caffeineMaxSize;
    }

    public void setCaffeineMaxSize(int caffeineMaxSize) {
        this.caffeineMaxSize = caffeineMaxSize;
    }

    public int getCaffeineExpireTime() {
        return caffeineExpireTime;
    }

    public void setCaffeineExpireTime(int caffeineExpireTime) {
        this.caffeineExpireTime = caffeineExpireTime;
    }

    public int getJsoupTimeout() {
        return jsoupTimeout;
    }

    public void setJsoupTimeout(int jsoupTimeout) {
        this.jsoupTimeout = jsoupTimeout;
    }
}
