package in.nimbo.common.config;

import in.nimbo.common.exception.LoadConfigurationException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.util.Properties;

public class KafkaConfig {
    private static final String CONFIG_NAME = "kafka.properties";
    private Properties consumerProperties;
    private Properties producerProperties;
    private String kafkaTopic;
    private int producerCount;
    private int localQueueSize;

    public static KafkaConfig load() {
        KafkaConfig config = new KafkaConfig();
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_NAME);
            config.setKafkaTopic(configuration.getString("topic.name"));
            config.setProducerCount(configuration.getInt("producer.count"));
            config.setLocalQueueSize(configuration.getInt("local.queue.size"));
            config.setConsumerProperties(Config.loadProperties("kafka-consumer.properties"));
            config.setProducerProperties(Config.loadProperties("kafka-producer.properties"));
            return config;
        } catch (ConfigurationException e) {
            throw new LoadConfigurationException(CONFIG_NAME, e);
        }
    }

    public int getLocalQueueSize() {
        return localQueueSize;
    }

    public void setLocalQueueSize(int localQueueSize) {
        this.localQueueSize = localQueueSize;
    }

    public int getProducerCount() {
        return producerCount;
    }

    public void setProducerCount(int producerCount) {
        this.producerCount = producerCount;
    }

    public Properties getConsumerProperties() {
        return consumerProperties;
    }

    public void setConsumerProperties(Properties consumerProperties) {
        this.consumerProperties = consumerProperties;
    }

    public Properties getProducerProperties() {
        return producerProperties;
    }

    public void setProducerProperties(Properties producerProperties) {
        this.producerProperties = producerProperties;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }
}
