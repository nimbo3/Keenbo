package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class SparkConfig {
    private static final String CONFIG_NAME = "spark.properties";
    private int port;
    private int minEdge;
    private int maxEdge;
    private int filterEdge;
    private double minNode;
    private double maxNode;
    private double filterNode;

    public static SparkConfig load() {
        SparkConfig sparkConfig = new SparkConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            sparkConfig.setPort(config.getInt("spark.port"));
            sparkConfig.setMinNode(config.getDouble("graph.sites.nodes.min"));
            sparkConfig.setMaxNode(config.getDouble("graph.sites.nodes.max"));
            sparkConfig.setFilterNode(config.getDouble("graph.sites.nodes.filter"));
            sparkConfig.setMinEdge(config.getInt("graph.sites.edges.min"));
            sparkConfig.setMaxEdge(config.getInt("graph.sites.edges.max"));
            sparkConfig.setFilterEdge(config.getInt("graph.sites.edges.filter"));
            return sparkConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public int getMinEdge() {
        return minEdge;
    }

    public void setMinEdge(int minEdge) {
        this.minEdge = minEdge;
    }

    public int getMaxEdge() {
        return maxEdge;
    }

    public void setMaxEdge(int maxEdge) {
        this.maxEdge = maxEdge;
    }

    public int getFilterEdge() {
        return filterEdge;
    }

    public void setFilterEdge(int filterEdge) {
        this.filterEdge = filterEdge;
    }

    public double getMinNode() {
        return minNode;
    }

    public void setMinNode(double minNode) {
        this.minNode = minNode;
    }

    public double getMaxNode() {
        return maxNode;
    }

    public void setMaxNode(double maxNode) {
        this.maxNode = maxNode;
    }

    public double getFilterNode() {
        return filterNode;
    }

    public void setFilterNode(double filterNode) {
        this.filterNode = filterNode;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return port;
    }
}
