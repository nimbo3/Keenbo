package in.nimbo.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ClassifierConfig {
    private static final String CONFIG_NAME = "classifier.properties";
    private String appName;
    private String esNodes;
    private String esWriteOperation;
    private String esIndexAutoCreate;
    private String esIndex;
    private String esType;
    private String naiveBayesModelType;
    private String naiveBayesModelSaveLocation;
    private int hashingNumFeatures;

    public static ClassifierConfig load() {
        ClassifierConfig pageRankConfig = new ClassifierConfig();
        try {
            PropertiesConfiguration config = new PropertiesConfiguration(CONFIG_NAME);
            pageRankConfig.setAppName(config.getString("app.name"));
            pageRankConfig.setEsNodes(config.getString("es.nodes"));
            pageRankConfig.setEsWriteOperation(config.getString("es.write.operation"));
            pageRankConfig.setEsIndexAutoCreate(config.getString("es.index.auto.create"));
            pageRankConfig.setEsIndex(config.getString("es.index"));
            pageRankConfig.setEsType(config.getString("es.type"));
            pageRankConfig.setNaiveBayesModelType(config.getString("naive.bayes.model.type"));
            pageRankConfig.setHashingNumFeatures(config.getInt("hashing.num.features"));
            pageRankConfig.setNaiveBayesModelSaveLocation(config.getString("naive.bayes.model.save.location"));
            return pageRankConfig;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
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
}
