package in.nimbo.config;

import in.nimbo.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class AppConfig {
    private static final String CONFIG_NAME = "app-config.properties";
    private int caffeineMaxSize;
    private int caffeineExpireTime;
    private int jsoupTimeout;
    private String jsoupUserAgent;
    private double englishProbability;
    private String reportName;
    private String reportHost;
    private int reportPort;
    private int reportPeriod;

    public static AppConfig load() {
        AppConfig appConfig = new AppConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            appConfig.setCaffeineMaxSize(config.getInt("caffeine.max.size"));
            appConfig.setCaffeineExpireTime(config.getInt("caffeine.expire.time"));
            appConfig.setJsoupTimeout(config.getInt("jsoup.timeout"));
            appConfig.setJsoupUserAgent(config.getString("jsoup.user.agent"));
            appConfig.setEnglishProbability(config.getDouble("english.probability"));
            appConfig.setReportName(config.getString("report.name"));
            appConfig.setReportHost(config.getString("report.host"));
            appConfig.setReportPort(config.getInt("report.port"));
            appConfig.setReportPeriod(config.getInt("report.period"));
            return appConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getReportName() {
        return reportName;
    }

    public void setReportName(String reportName) {
        this.reportName = reportName;
    }

    public String getReportHost() {
        return reportHost;
    }

    public void setReportHost(String reportHost) {
        this.reportHost = reportHost;
    }

    public int getReportPort() {
        return reportPort;
    }

    public void setReportPort(int reportPort) {
        this.reportPort = reportPort;
    }

    public int getReportPeriod() {
        return reportPeriod;
    }

    public void setReportPeriod(int reportPeriod) {
        this.reportPeriod = reportPeriod;
    }

    public String getJsoupUserAgent() {
        return jsoupUserAgent;
    }

    public void setJsoupUserAgent(String jsoupUserAgent) {
        this.jsoupUserAgent = jsoupUserAgent;
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

    public double getEnglishProbability() {
        return englishProbability;
    }

    public void setEnglishProbability(double englishProbability) {
        this.englishProbability = englishProbability;
    }
}
