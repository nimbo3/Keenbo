package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.KafkaServiceException;
import in.nimbo.service.CollectorService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class KafkaServiceImpl implements KafkaService {
    private Logger logger = LoggerFactory.getLogger("app");
    private KafkaConfig config;
    private CollectorService collectorService;

    private BlockingQueue<Page> messageQueue;
    private ConsumerService consumerService;
    private List<ProducerService> producerServices;
    private CountDownLatch countDownLatch;

    public KafkaServiceImpl(KafkaConfig kafkaConfig, CollectorService collectorService) {
        this.config = kafkaConfig;
        producerServices = new ArrayList<>();
        countDownLatch = new CountDownLatch(kafkaConfig.getPageProducerCount() + 1);
        this.collectorService = collectorService;
    }

    /**
     * prepare kafka producer and consumer services and start threads to send/receive messages
     *
     * @throws KafkaServiceException if unable to prepare services
     */
    @Override
    public void schedule() {
        ExecutorService executorService = Executors.newFixedThreadPool(config.getPageProducerCount() + 1);
        messageQueue = new ArrayBlockingQueue<>(config.getLocalPageQueueSize());

        // Prepare consumer
        KafkaConsumer<String, Page> kafkaConsumer = new KafkaConsumer<>(config.getPageConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(config.getPageTopic()));
        consumerService = new ConsumerServiceImpl(kafkaConsumer, messageQueue, countDownLatch);
        executorService.submit(consumerService);

        // Prepare producer
        for (int i = 0; i < config.getPageProducerCount(); i++) {
            KafkaProducer<String, Page> producer = new KafkaProducer<>(config.getPageProducerProperties());
            ProducerService producerService =
                    new ProducerServiceImpl(config,
                            messageQueue, producer,
                            collectorService,
                            countDownLatch);
            producerServices.add(producerService);
            executorService.submit(producerService);
        }
        executorService.shutdown();
    }

    /**
     * stop services
     */
    @Override
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
            try (KafkaProducer<String, Page> producer = new KafkaProducer<>(config.getPageProducerProperties())) {
                for (Page page : messageQueue) {
                    producer.send(new ProducerRecord<>(config.getPageTopic(), page.getLink(), page));
                }
                producer.flush();
            }
            logger.info("All messages sent to kafka");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
