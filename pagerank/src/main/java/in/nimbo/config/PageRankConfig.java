package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class PageRankConfig {
    private static final String CONFIG_NAME = "pagerank.properties";
    private String resourceManager;
    private String appName;
    private double resetProbability;
    private int maxIter;
    private String esNodes;
    private String esWriteOperation;
    private String esMappingId;
    private String esIndexAutoCreate;
    private String esIndex;
    private String esType;

    public static PageRankConfig load() {
        PageRankConfig pageRankConfig = new PageRankConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            pageRankConfig.setResourceManager(config.getString("resource.manager"));
            pageRankConfig.setAppName(config.getString("app.name"));
            pageRankConfig.setMaxIter(config.getInt("algorithm.max.iter"));
            pageRankConfig.setResetProbability(config.getDouble("algorithm.reset.probability"));
            pageRankConfig.setEsNodes(config.getString("es.nodes"));
            pageRankConfig.setEsWriteOperation(config.getString("es.write.operation"));
            pageRankConfig.setEsIndexAutoCreate(config.getString("es.index.auto.create"));
            pageRankConfig.setEsIndex(config.getString("es.index"));
            pageRankConfig.setEsType(config.getString("es.type"));
            return pageRankConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public String getEsIndex() {
        return esIndex;
    }

    public void setEsIndex(String esIndex) {
        this.esIndex = esIndex;
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

    public String getEsNodes() {
        return esNodes;
    }

    public void setEsNodes(String esNodes) {
        this.esNodes = esNodes;
    }

    public String getEsWriteOperation() {
        return esWriteOperation;
    }

    public void setEsWriteOperation(String esWriteOperation) {
        this.esWriteOperation = esWriteOperation;
    }

    public String getEsIndexAutoCreate() {
        return esIndexAutoCreate;
    }

    public void setEsIndexAutoCreate(String esIndexAutoCreate) {
        this.esIndexAutoCreate = esIndexAutoCreate;
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

}
