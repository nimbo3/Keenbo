package in.nimbo.service.kafka;

import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaService {
    private ExecutorService executorService;
    private Properties producerProperties;
    private Properties consumerProperties;
    private CrawlerService crawlerService;
    private static final int CONSUMER_COUNT = 2;
    static final String KAFKA_TOPIC = "links";

    public KafkaService(CrawlerService crawlerService) {
        this.crawlerService = crawlerService;
        executorService = Executors.newFixedThreadPool(CONSUMER_COUNT);
    }

    private void loadProperties() throws IOException {
        if (producerProperties == null || consumerProperties == null) {
            consumerProperties = new Properties();
            producerProperties = new Properties();
            ClassLoader classLoader = KafkaService.class.getClassLoader();
            consumerProperties.load(classLoader.getResourceAsStream("kafka-consumer.properties"));
            producerProperties.load(classLoader.getResourceAsStream("kafka-producer.properties"));
        }
    }

    /**
     * prepare kafka producer and consumer services and start threads to send/receive messages
     * @throws RuntimeException if unable to prepare services
     */
    public void schedule() {
        try {
            loadProperties();
            for (int i = 0; i < CONSUMER_COUNT; i++) {
                KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
                consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
                executorService.submit(new KafkaProducerConsumer(producer, consumer, crawlerService));
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to load kafka service", e);
        }
    }

    public void sendMessage(String message) {
        try {
            loadProperties();
            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
            producer.send(new ProducerRecord<>(KAFKA_TOPIC, "Producer message", message));
        } catch (IOException e) {
            throw new RuntimeException("Unable to load kafka service", e);
        }
    }
}
