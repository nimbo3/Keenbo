package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.KafkaServiceException;
import in.nimbo.monitoring.ThreadsMonitor;
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

public class KafkaServiceImpl implements KafkaService {
    private Logger logger = LoggerFactory.getLogger("app");
    private KafkaConfig kafkaConfig;
    private CrawlerService crawlerService;
    private BlockingQueue<String> messageQueue;
    private ConsumerService consumerService;
    private List<ProducerService> producerServices;
    private CountDownLatch countDownLatch;
    private ThreadsMonitor threadsMonitor;

    public KafkaServiceImpl(CrawlerService crawlerService, KafkaConfig kafkaConfig) {
        this.crawlerService = crawlerService;
        this.kafkaConfig = kafkaConfig;
        producerServices = new ArrayList<>();
        countDownLatch = new CountDownLatch(kafkaConfig.getLinkProducerCount() + 1);
    }

    /**
     * prepare kafka producer and consumer services and start threads to send/receive messages
     *
     * @throws KafkaServiceException if unable to prepare services
     */
    @Override
    public void schedule() {
        final ThreadGroup threadGroup = new ThreadGroup("workers");
        int numberOfThreads = kafkaConfig.getLinkProducerCount() + 1;
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(numberOfThreads, numberOfThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), r -> new Thread(threadGroup, r));
        startThreadsMonitoring(executorService, threadGroup);

        messageQueue = new ArrayBlockingQueue<>(kafkaConfig.getLocalLinkQueueSize());
        // Prepare consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getLinkConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getLinkTopic()));
        consumerService = new ConsumerServiceImpl(kafkaConsumer, messageQueue, countDownLatch);
        executorService.submit(consumerService);

        // Prepare producer
        for (int i = 0; i < kafkaConfig.getLinkProducerCount(); i++) {
            KafkaProducer<String, String> linkProducer = new KafkaProducer<>(kafkaConfig.getLinkProducerProperties());
            KafkaProducer<String, Page> pageProducer = new KafkaProducer<>(kafkaConfig.getPageProducerProperties());
            ProducerService producerService =
                    new ProducerServiceImpl(kafkaConfig, messageQueue,
                            linkProducer, pageProducer,
                            crawlerService,
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
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getLinkProducerProperties())) {
                for (String message : messageQueue) {
                    producer.send(new ProducerRecord<>(kafkaConfig.getLinkTopic(), message, message));
                }
                producer.flush();
            }
            logger.info("All messages sent to kafka");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void sendMessage(String message) {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getLinkProducerProperties())) {
            producer.send(new ProducerRecord<>(kafkaConfig.getLinkTopic(), message, message));
            producer.flush();
        }
    }

    private void startThreadsMonitoring(ThreadPoolExecutor threadPoolExecutor, ThreadGroup threadGroup) {
        threadsMonitor = new ThreadsMonitor(threadPoolExecutor, threadGroup);
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleAtFixedRate(threadsMonitor, 0, 15, TimeUnit.SECONDS);
        scheduledExecutorService.shutdown();
    }
}
