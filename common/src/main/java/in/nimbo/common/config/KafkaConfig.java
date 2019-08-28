package in.nimbo.common.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Properties;

public class KafkaConfig {
    private static final String CONFIG_NAME = "kafka/kafka.properties";
    private Properties linkConsumerProperties;
    private Properties pageConsumerProperties;
    private Properties shufflerConsumerProperties;
    private Properties trainingConsumerProperties;
    private Properties pageProducerProperties;
    private Properties linkProducerProperties;
    private Properties shufflerProducerProperties;
    private Properties trainingProducerProperties;
    private String serviceName;
    private String linkTopic;
    private String pageTopic;
    private String shufflerTopic;
    private int maxPollDuration;
    private int pageProducerCount;
    private int linkProducerCount;
    private int shufflerProducerCount;
    private int localLinkQueueSize;
    private int localPageQueueSize;
    private String trainingTopic;

    public static KafkaConfig load() {
        KafkaConfig config = new KafkaConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setLinkConsumerProperties(Config.loadProperties("kafka/link-consumer.properties"));
            config.setPageConsumerProperties(Config.loadProperties("kafka/page-consumer.properties"));
            config.setLinkProducerProperties(Config.loadProperties("kafka/link-producer.properties"));
            config.setPageProducerProperties(Config.loadProperties("kafka/page-producer.properties"));
            config.setShufflerConsumerProperties(Config.loadProperties("kafka/shuffler-consumer.properties"));
            config.setTrainingConsumerProperties(Config.loadProperties("kafka/training-consumer.properties"));
            config.setShufflerProducerProperties(Config.loadProperties("kafka/shuffler-producer.properties"));
            config.setTrainingProducerProperties(Config.loadProperties("kafka/training-producer.properties"));
            config.setServiceName(configuration.getString("service.name"));
            config.setLinkTopic(configuration.getString("link.topic.name"));
            config.setPageTopic(configuration.getString("page.topic.name"));
            config.setTrainingTopic(configuration.getString("training.topic.name"));
            config.setShufflerTopic(configuration.getString("shuffler.topic.name"));
            config.setMaxPollDuration(configuration.getInt("max.poll.duration.milliseconds"));
            config.setPageProducerCount(configuration.getInt("page.producer.count"));
            config.setLinkProducerCount(configuration.getInt("link.producer.count"));
            config.setShufflerProducerCount(configuration.getInt("shuffler.producer.count"));
            config.setLocalLinkQueueSize(configuration.getInt("local.link.queue.size"));
            config.setLocalPageQueueSize(configuration.getInt("local.page.queue.size"));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public Properties getTrainingConsumerProperties() {
        return trainingConsumerProperties;
    }

    public void setTrainingConsumerProperties(Properties trainingConsumerProperties) {
        this.trainingConsumerProperties = trainingConsumerProperties;
    }

    public Properties getTrainingProducerProperties() {
        return trainingProducerProperties;
    }

    public void setTrainingProducerProperties(Properties trainingProducerProperties) {
        this.trainingProducerProperties = trainingProducerProperties;
    }

    public int getMaxPollDuration() {
        return maxPollDuration;
    }

    public void setMaxPollDuration(int maxPollDuration) {
        this.maxPollDuration = maxPollDuration;
    }

    public Properties getShufflerConsumerProperties() {
        return shufflerConsumerProperties;
    }

    public void setShufflerConsumerProperties(Properties shufflerConsumerProperties) {
        this.shufflerConsumerProperties = shufflerConsumerProperties;
    }

    public Properties getShufflerProducerProperties() {
        return shufflerProducerProperties;
    }

    public void setShufflerProducerProperties(Properties shufflerProducerProperties) {
        this.shufflerProducerProperties = shufflerProducerProperties;
    }

    public String getShufflerTopic() {
        return shufflerTopic;
    }

    public void setShufflerTopic(String shufflerTopic) {
        this.shufflerTopic = shufflerTopic;
    }

    public int getShufflerProducerCount() {
        return shufflerProducerCount;
    }

    public void setShufflerProducerCount(int shufflerProducerCount) {
        this.shufflerProducerCount = shufflerProducerCount;
    }

    public Properties getLinkConsumerProperties() {
        return linkConsumerProperties;
    }

    public void setLinkConsumerProperties(Properties linkConsumerProperties) {
        this.linkConsumerProperties = linkConsumerProperties;
    }

    public Properties getPageConsumerProperties() {
        return pageConsumerProperties;
    }

    public void setPageConsumerProperties(Properties pageConsumerProperties) {
        this.pageConsumerProperties = pageConsumerProperties;
    }

    public Properties getPageProducerProperties() {
        return pageProducerProperties;
    }

    public void setPageProducerProperties(Properties pageProducerProperties) {
        this.pageProducerProperties = pageProducerProperties;
    }

    public Properties getLinkProducerProperties() {
        return linkProducerProperties;
    }

    public void setLinkProducerProperties(Properties linkProducerProperties) {
        this.linkProducerProperties = linkProducerProperties;
    }

    public String getLinkTopic() {
        return linkTopic;
    }

    public void setLinkTopic(String linkTopic) {
        this.linkTopic = linkTopic;
    }

    public String getPageTopic() {
        return pageTopic;
    }

    public void setPageTopic(String pageTopic) {
        this.pageTopic = pageTopic;
    }

    public int getPageProducerCount() {
        return pageProducerCount;
    }

    public void setPageProducerCount(int pageProducerCount) {
        this.pageProducerCount = pageProducerCount;
    }

    public int getLinkProducerCount() {
        return linkProducerCount;
    }

    public void setLinkProducerCount(int linkProducerCount) {
        this.linkProducerCount = linkProducerCount;
    }

    public int getLocalLinkQueueSize() {
        return localLinkQueueSize;
    }

    public void setLocalLinkQueueSize(int localLinkQueueSize) {
        this.localLinkQueueSize = localLinkQueueSize;
    }

    public int getLocalPageQueueSize() {
        return localPageQueueSize;
    }

    public void setLocalPageQueueSize(int localPageQueueSize) {
        this.localPageQueueSize = localPageQueueSize;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getTrainingTopic() {
        return trainingTopic;
    }

    public void setTrainingTopic(String trainingTopic) {
        this.trainingTopic = trainingTopic;
    }
}
