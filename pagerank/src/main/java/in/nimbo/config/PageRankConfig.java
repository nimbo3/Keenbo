package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class PageRankConfig {
    private static final String CONFIG_NAME = "pagerank.properties";
    private String resourceManager;
    private String appName;
    private String hBaseTable;
    private String hBaseColumnFamily;
    private double resetProbability;
    private int maxIter;

    public static PageRankConfig load() {
        PageRankConfig pageRankConfig = new PageRankConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            pageRankConfig.setResourceManager(config.getString("resource.manager"));
            pageRankConfig.setAppName(config.getString("app.name"));
            pageRankConfig.sethBaseTable(config.getString("hBase.table"));
            pageRankConfig.sethBaseColumnFamily(config.getString("hBase.column.family"));
            pageRankConfig.setMaxIter(config.getInt("algorithm.max.iter"));
            pageRankConfig.setResetProbability(config.getDouble("algorithm.reset.probability"));
            return pageRankConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public double getResetProbability() {
        return resetProbability;
    }

    public void setResetProbability(double resetProbability) {
        this.resetProbability = resetProbability;
    }

    public int getMaxIter() {
        return maxIter;
    }

    public void setMaxIter(int maxIter) {
        this.maxIter = maxIter;
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

    public String gethBaseTable() {
        return hBaseTable;
    }

    public void sethBaseTable(String hBaseTable) {
        this.hBaseTable = hBaseTable;
    }

    public String gethBaseColumnFamily() {
        return hBaseColumnFamily;
    }

    public void sethBaseColumnFamily(String hBaseColumnFamily) {
        this.hBaseColumnFamily = hBaseColumnFamily;
    }

}
