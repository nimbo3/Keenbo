package in.nimbo.common.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ProjectConfig {
    private static final String CONFIG_NAME = "project-config.properties";
    private int caffeineMaxSize;
    private int caffeineExpireTime;
    private int jsoupTimeout;
    private String jsoupUserAgent;
    private double englishProbability;
    private String reportName;
    private String reportHost;
    private int reportPort;
    private int reportPeriod;
    private int monitoringPeriod;

    public static ProjectConfig load() {
        ProjectConfig projectConfig = new ProjectConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            projectConfig.setCaffeineMaxSize(config.getInt("caffeine.max.size"));
            projectConfig.setCaffeineExpireTime(config.getInt("caffeine.expire.time"));
            projectConfig.setJsoupTimeout(config.getInt("jsoup.timeout"));
            projectConfig.setJsoupUserAgent(config.getString("jsoup.user.agent"));
            projectConfig.setEnglishProbability(config.getDouble("english.probability"));
            projectConfig.setReportName(config.getString("report.name"));
            projectConfig.setReportHost(config.getString("report.host"));
            projectConfig.setReportPort(config.getInt("report.port"));
            projectConfig.setReportPeriod(config.getInt("report.period"));
            projectConfig.setMonitoringPeriod(config.getInt("monitoring.period.seconds"));
            return projectConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public int getMonitoringPeriod() {
        return monitoringPeriod;
    }

    public void setMonitoringPeriod(int monitoringPeriod) {
        this.monitoringPeriod = monitoringPeriod;
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
