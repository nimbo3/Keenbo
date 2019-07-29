package in.nimbo.service.kafka;

import in.nimbo.config.KafkaConfig;
import in.nimbo.exception.KafkaServiceException;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class KafkaService {
    private Logger logger = LoggerFactory.getLogger(KafkaService.class);
    private KafkaConfig kafkaConfig;
    private CrawlerService crawlerService;
    private BlockingQueue<String> messageQueue;
    private ConsumerService consumerService;
    private List<ProducerService> producerServices;
    private CountDownLatch countDownLatch;

    public KafkaService(CrawlerService crawlerService, KafkaConfig kafkaConfig) {
        this.crawlerService = crawlerService;
        this.kafkaConfig = kafkaConfig;
        producerServices = new ArrayList<>();
        countDownLatch = new CountDownLatch(kafkaConfig.getProducerCount() + 1);
    }

    /**
     * prepare kafka producer and consumer services and start threads to send/receive messages
     *
     * @throws KafkaServiceException if unable to prepare services
     */
    public void schedule() {
        ExecutorService executorService = Executors.newFixedThreadPool(kafkaConfig.getProducerCount() + 1);
        messageQueue = new ArrayBlockingQueue<>(kafkaConfig.getLocalQueueSize());

        // Prepare consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getKafkaTopic()));
        consumerService = new ConsumerService(kafkaConsumer, messageQueue, countDownLatch);
        executorService.submit(consumerService);

        // Prepare producer
        for (int i = 0; i < kafkaConfig.getProducerCount(); i++) {
            KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getProducerProperties());
            ProducerService producerService = new ProducerService(producer, kafkaConfig.getKafkaTopic(), messageQueue, crawlerService, countDownLatch);
            producerServices.add(producerService);
            executorService.submit(producerService);
        }
        executorService.shutdown();
    }

    /**
     * stop services
     */
    public void stopSchedule() {
        logger.info("Stop schedule service");

        consumerService.close();
        for (ProducerService producerService : producerServices) {
            producerService.close();
        }
        try {
            countDownLatch.await();
            logger.info("All service stopped");
            logger.info("Start sending {} messages to kafka", messageQueue.size());
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getProducerProperties())) {
                for (String message : messageQueue) {
                    producer.send(new ProducerRecord<>(kafkaConfig.getKafkaTopic(), message, message));
                }
                producer.flush();
            }
            logger.info("All messages sent to kafka");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * send a message to kafka
     *
     * @param message message value
     */
    public void sendMessage(String message) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getProducerProperties())) {
            producer.send(new ProducerRecord<>(kafkaConfig.getKafkaTopic(), message, message));
            producer.flush();
            System.out.println("Site " + message + " added");
        }
    }
}
