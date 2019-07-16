package in.nimbo.service.kafka;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaServiceImpl {
    private ExecutorService executorService;
    private Properties producerProperties;
    private Properties consumerProperties;
    private static final int CONSUMER_COUNT = 100;

    public KafkaServiceImpl() {
        executorService = Executors.newFixedThreadPool(CONSUMER_COUNT);
    }

    private void loadProperties() throws IOException {
        if (producerProperties == null || consumerProperties == null) {
            ClassLoader classLoader = KafkaServiceImpl.class.getClassLoader();
            consumerProperties.load(classLoader.getResourceAsStream("kafka-consumer.properties"));
            producerProperties.load(classLoader.getResourceAsStream("kafka-producer.properties"));
        }
    }

    public void schedule() {

    }
}
