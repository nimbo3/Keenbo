package in.nimbo.service.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaServiceImpl {
    private ExecutorService executorService;
    private Properties producerProperties;
    private Properties consumerProperties;
    private static final int CONSUMER_COUNT = 1;
    private static final String KAFKA_TOPIC = "links";

    public KafkaServiceImpl() {
        executorService = Executors.newFixedThreadPool(CONSUMER_COUNT);
    }

    private void loadProperties() throws IOException {
        if (producerProperties == null || consumerProperties == null) {
            consumerProperties = new Properties();
            producerProperties = new Properties();
            ClassLoader classLoader = KafkaServiceImpl.class.getClassLoader();
            consumerProperties.load(classLoader.getResourceAsStream("kafka-consumer.properties"));
            producerProperties.load(classLoader.getResourceAsStream("kafka-producer.properties"));
        }
    }

    public void schedule() {
        try {
            loadProperties();
            for (int i = 0; i < CONSUMER_COUNT; i++) {
                KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
                consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
                executorService.submit(new KafkaProducerConsumer(producer, consumer));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new KafkaServiceImpl().schedule();
    }
}
