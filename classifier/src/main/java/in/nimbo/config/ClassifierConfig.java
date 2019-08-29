package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ClassifierConfig {
    public enum MODE {
        CRAWL, CLASSIFY
    }

    private static final String CONFIG_NAME = "classifier.properties";
    private String appName;
    private MODE appMode;
    private String esNodes;
    private String esWriteOperation;
    private String esIndexAutoCreate;
    private String esIndex;
    private String esType;
    private String naiveBayesModelType;
    private String naiveBayesModelSaveLocation;
    private int hashingNumFeatures;
    private int crawlerThreads;
    private int crawlerLevel;
    private int crawlerQueueSize;

    public static ClassifierConfig load() {
        ClassifierConfig classifierConfig = new ClassifierConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            classifierConfig.setAppName(config.getString("app.name"));
            if (config.getString("app.mode").equals("crawl")) {
                classifierConfig.setAppMode(MODE.CRAWL);
            }
            else if (config.getString("app.mode").equals("classify")){
                classifierConfig.setAppMode(MODE.CLASSIFY);
            }
            else {
                throw new ConfigurationException("Invalid App mode");
            }
            classifierConfig.setEsNodes(config.getString("es.nodes"));
            classifierConfig.setEsWriteOperation(config.getString("es.write.operation"));
            classifierConfig.setEsIndexAutoCreate(config.getString("es.index.auto.create"));
            classifierConfig.setEsIndex(config.getString("es.index"));
            classifierConfig.setEsType(config.getString("es.type"));
            classifierConfig.setNaiveBayesModelType(config.getString("naive.bayes.model.type"));
            classifierConfig.setHashingNumFeatures(config.getInt("hashing.num.features"));
            classifierConfig.setNaiveBayesModelSaveLocation(config.getString("naive.bayes.model.save.location"));
            classifierConfig.setCrawlerLevel(config.getInt("crawler.level"));
            classifierConfig.setCrawlerThreads(config.getInt("crawler.threads"));
            classifierConfig.setCrawlerQueueSize(config.getInt("crawler.queue.size"));
            return classifierConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public int getCrawlerQueueSize() {
        return crawlerQueueSize;
    }

    public void setCrawlerQueueSize(int crawlerQueueSize) {
        this.crawlerQueueSize = crawlerQueueSize;
    }

    public int getCrawlerThreads() {
        return crawlerThreads;
    }

    public void setCrawlerThreads(int crawlerThreads) {
        this.crawlerThreads = crawlerThreads;
    }

    public int getCrawlerLevel() {
        return crawlerLevel;
    }

    public void setCrawlerLevel(int crawlerLevel) {
        this.crawlerLevel = crawlerLevel;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
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

    public String getNaiveBayesModelType() {
        return naiveBayesModelType;
    }

    public void setNaiveBayesModelType(String naiveBayesModelType) {
        this.naiveBayesModelType = naiveBayesModelType;
    }

    public int getHashingNumFeatures() {
        return hashingNumFeatures;
    }

    public void setHashingNumFeatures(int hashingNumFeatures) {
        this.hashingNumFeatures = hashingNumFeatures;
    }

    public String getNaiveBayesModelSaveLocation() {
        return naiveBayesModelSaveLocation;
    }

    public void setNaiveBayesModelSaveLocation(String naiveBayesModelSaveLocation) {
        this.naiveBayesModelSaveLocation = naiveBayesModelSaveLocation;
    }

    public MODE getAppMode() {
        return appMode;
    }

    public void setAppMode(MODE appMode) {
        this.appMode = appMode;
    }
}
