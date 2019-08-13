package in.nimbo.common.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Properties;

public class KafkaConfig {
    private static final String CONFIG_NAME = "kafka/kafka.properties";
    private Properties linkConsumerProperties;
    private Properties pageConsumerProperties;
    private Properties pageProducerProperties;
    private Properties linkProducerProperties;
    private String linkTopic;
    private String pageTopic;
    private int pageProducerCount;
    private int linkProducerCount;
    private int localLinkQueueSize;

    public static KafkaConfig load() {
        KafkaConfig config = new KafkaConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setLinkConsumerProperties(Config.loadProperties("kafka/link-consumer.properties"));
            config.setPageConsumerProperties(Config.loadProperties("kafka/page-consumer.properties"));
            config.setLinkProducerProperties(Config.loadProperties("kafka/link-producer.properties"));
            config.setPageProducerProperties(Config.loadProperties("kafka/page-producer.properties"));
            config.setLinkTopic(configuration.getString("link.topic.name"));
            config.setPageTopic(configuration.getString("page.topic.name"));
            config.setPageProducerCount(configuration.getInt("page.producer.count"));
            config.setLinkProducerCount(configuration.getInt("link.producer.count"));
            config.setLocalLinkQueueSize(configuration.getInt("local.queue.size"));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
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
}
