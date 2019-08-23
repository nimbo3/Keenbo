package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ShufflerConfig {
    private static final String CONFIG_NAME = "shuffler-config.properties";
    private int shuffleSize;
    private int shuffleWaitMinutes;

    public static ShufflerConfig load() {
        ShufflerConfig pageRankConfig = new ShufflerConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            pageRankConfig.setShuffleSize(config.getInt("shuffle.size"));
            pageRankConfig.setShuffleWaitMinutes(config.getInt("shuffle.wait.minutes"));
            return pageRankConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public int getShuffleSize() {
        return shuffleSize;
    }

    public void setShuffleSize(int shuffleSize) {
        this.shuffleSize = shuffleSize;
    }

    public int getShuffleWaitMinutes() {
        return shuffleWaitMinutes;
    }

    public void setShuffleWaitMinutes(int shuffleWaitMinutes) {
        this.shuffleWaitMinutes = shuffleWaitMinutes;
    }
}
