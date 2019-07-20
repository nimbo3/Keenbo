package in.nimbo.service.kafka;

import in.nimbo.exception.KafkaServiceException;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaService {
    private Logger logger = LoggerFactory.getLogger(KafkaService.class);
    private ExecutorService executorService;
    private Properties producerProperties;
    private Properties consumerProperties;
    private CrawlerService crawlerService;
    private List<Consumer> consumers;
    private static final int CONSUMER_COUNT = 1;
    static final String KAFKA_TOPIC = "links";

    public KafkaService(CrawlerService crawlerService) {
        this.crawlerService = crawlerService;
        consumers = new ArrayList<>();
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
     * @throws KafkaServiceException if unable to prepare services
     */
    public void schedule() {
        try {
            loadProperties();
            logger.info("Start kafka schedule service");
            for (int i = 0; i < CONSUMER_COUNT; i++) {
                KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
                consumers.add(consumer);
                consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));
                executorService.submit(new KafkaProducerConsumer(producer, consumer, crawlerService));
            }
        } catch (IOException e) {
            throw new KafkaServiceException(e);
        }
    }

    public void stopSchedule() {
        for (Consumer consumer : consumers) {
            consumer.wakeup();
        }
        executorService.shutdown();
    }

    public void sendMessage(String message) {
        try {
            loadProperties();
            KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
            producer.send(new ProducerRecord<>(KAFKA_TOPIC, "Producer message", message));
            producer.flush();
        } catch (IOException e) {
            throw new KafkaServiceException(e);
        }
    }
}
