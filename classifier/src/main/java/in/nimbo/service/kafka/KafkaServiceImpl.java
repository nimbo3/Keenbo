package in.nimbo.service.kafka;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Link;
import in.nimbo.common.monitoring.ThreadsMonitor;
import in.nimbo.config.ClassifierConfig;
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
    private Logger logger = LoggerFactory.getLogger("collector");
    private KafkaConfig kafkaConfig;
    private ClassifierConfig classifierConfig;
    private CrawlerService crawlerService;

    private ScheduledExecutorService threadMonitorService;
    private BlockingQueue<Link> messageQueue;
    private ConsumerService consumerService;

    private List<ProducerService> producerServices;
    private List<Thread> kafkaServices;
    private CountDownLatch countDownLatch;

    public KafkaServiceImpl(KafkaConfig kafkaConfig, ClassifierConfig classifierConfig, CrawlerService crawlerService) {
        this.kafkaConfig = kafkaConfig;
        this.classifierConfig = classifierConfig;
        this.crawlerService = crawlerService;
        producerServices = new ArrayList<>();
        kafkaServices = new ArrayList<>();
        messageQueue = new ArrayBlockingQueue<>(classifierConfig.getCrawlerQueueSize());
        countDownLatch = new CountDownLatch(kafkaConfig.getPageProducerCount() + 1);
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        metricRegistry.register(MetricRegistry.name(KafkaServiceImpl.class, "localPageQueueSize"),
                new CachedGauge<Integer>(15, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return messageQueue.size();
                    }
                });
    }

    @Override
    public void schedule() {
        startThreadsMonitoring();

        KafkaConsumer<String, Link> kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getTrainingConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getTrainingTopic()));
        consumerService = new ConsumerServiceImpl(kafkaConfig, kafkaConsumer, messageQueue, countDownLatch);
        Thread consumerThread = new Thread(consumerService, kafkaConfig.getServiceName() + "0");
        kafkaServices.add(consumerThread);

        for (int i = 1; i <= classifierConfig.getCrawlerThreads(); i++) {
            KafkaProducer<String, Link> producer = new KafkaProducer<>(kafkaConfig.getTrainingProducerProperties());
            ProducerService producerService =
                    new ProducerServiceImpl(kafkaConfig, classifierConfig, crawlerService, messageQueue, producer, countDownLatch);
            Thread pageProducerThread = new Thread(producerService, kafkaConfig.getServiceName() + i);
            kafkaServices.add(pageProducerThread);
            producerServices.add(producerService);
        }
        for (Thread kafkaService : kafkaServices) {
            kafkaService.start();
        }
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
        for (Thread service : kafkaServices) {
            service.interrupt();
        }
        try {
            countDownLatch.await();
            logger.info("All service stopped");
            try (KafkaProducer<String, Link> producer = new KafkaProducer<>(kafkaConfig.getTrainingProducerProperties())) {
                logger.info("Start sending {} messages from local page queue to kafka", messageQueue.size());
                for (Link link : messageQueue) {
                    producer.send(new ProducerRecord<>(kafkaConfig.getTrainingTopic(), link));
                }
                producer.flush();
            }
            logger.info("All messages sent to kafka");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        threadMonitorService.shutdown();
    }

    private void startThreadsMonitoring() {
        ThreadsMonitor threadsMonitor = new ThreadsMonitor(kafkaServices);
        threadMonitorService = Executors.newScheduledThreadPool(1);
        threadMonitorService.scheduleAtFixedRate(threadsMonitor, 0, 1, TimeUnit.SECONDS);
    }
}
