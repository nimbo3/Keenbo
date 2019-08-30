package in.nimbo.service.kafka;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.monitoring.ThreadsMonitor;
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
    private Logger logger = LoggerFactory.getLogger("collector");
    private ScheduledExecutorService threadMonitorService;
    private KafkaConfig config;
    private CollectorService collectorService;

    private BlockingQueue<Page> messageQueue;
    private ConsumerService consumerService;

    private List<ProducerService> producerServices;
    private List<Thread> kafkaServices;
    private List<List<Page>> bufferLists;
    private CountDownLatch countDownLatch;

    public KafkaServiceImpl(KafkaConfig kafkaConfig, CollectorService collectorService) {
        this.config = kafkaConfig;
        this.collectorService = collectorService;
        producerServices = new ArrayList<>();
        kafkaServices = new ArrayList<>();
        bufferLists = new ArrayList<>();
        messageQueue = new ArrayBlockingQueue<>(config.getLocalPageQueueSize());
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

        KafkaConsumer<String, Page> kafkaConsumer = new KafkaConsumer<>(config.getPageConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(config.getPageTopic()));
        consumerService = new ConsumerServiceImpl(config, kafkaConsumer, messageQueue, countDownLatch);
        Thread consumerThread = new Thread(consumerService, config.getServiceName() + "0");
        kafkaServices.add(consumerThread);

        for (int i = 1; i <= config.getPageProducerCount(); i++) {
            List<Page> bufferList = new ArrayList<>();
            KafkaProducer<String, Page> producer = new KafkaProducer<>(config.getPageProducerProperties());
            ProducerService producerService =
                    new ProducerServiceImpl(config, messageQueue, bufferList, producer, collectorService, countDownLatch);
            Thread pageProducerThread = new Thread(producerService, config.getServiceName() + i);
            kafkaServices.add(pageProducerThread);
            producerServices.add(producerService);
            bufferLists.add(bufferList);
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
            countDownLatch.await(1, TimeUnit.MINUTES);
            logger.info("All service stopped");
            try (KafkaProducer<String, Page> producer = new KafkaProducer<>(config.getPageProducerProperties())) {
                logger.info("Start sending {} messages from local page queue to kafka", messageQueue.size());
                for (Page page : messageQueue) {
                    producer.send(new ProducerRecord<>(config.getPageTopic(), page));
                }
                for (List<Page> bufferList : bufferLists) {
                    logger.info("Start sending {} messages from local buffer list to kafka", messageQueue.size());
                    for (Page page : bufferList) {
                        producer.send(new ProducerRecord<>(config.getPageTopic(), page));
                    }
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
