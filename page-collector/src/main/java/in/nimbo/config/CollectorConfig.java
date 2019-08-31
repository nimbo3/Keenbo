package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class CollectorConfig {
    private static final String CONFIG_NAME = "page-collector.properties";
    private boolean extractKeywordEnabled;

    public static CollectorConfig load() {
        CollectorConfig pageRankConfig = new CollectorConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            pageRankConfig.setExtractKeywordEnabled(config.getBoolean("extract.keyword.enabled"));
            return pageRankConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public boolean isExtractKeywordEnabled() {
        return extractKeywordEnabled;
    }

    public void setExtractKeywordEnabled(boolean extractKeywordEnabled) {
        this.extractKeywordEnabled = extractKeywordEnabled;
    }
}
