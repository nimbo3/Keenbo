package in.nimbo.service.kafka;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.config.ShufflerConfig;
import in.nimbo.service.ShufflerService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaServiceImpl implements KafkaService {
    private Logger logger = LoggerFactory.getLogger("shuffler");
    private KafkaConfig config;
    private ShufflerConfig shufflerConfig;
    private List<String> shuffleList;
    private ShufflerService shufflerService;
    private Thread shufflerServiceThread;

    private CountDownLatch countDownLatch;

    public KafkaServiceImpl(KafkaConfig kafkaConfig, ShufflerConfig shufflerConfig) {
        this.config = kafkaConfig;
        this.shufflerConfig = shufflerConfig;
        countDownLatch = new CountDownLatch(1);
        shuffleList = new ArrayList<>();
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        metricRegistry.register(MetricRegistry.name(KafkaServiceImpl.class, "localShuffleQueueSize"),
                new CachedGauge<Integer>(15, TimeUnit.SECONDS) {
                    @Override
                    protected Integer loadValue() {
                        return shuffleList.size();
                    }
                });
    }

    @Override
    public void schedule() {
        KafkaConsumer<String, String> shufflerConsumer = new KafkaConsumer<>(config.getShufflerConsumerProperties());
        KafkaProducer<String, String> linkProducer = new KafkaProducer<>(config.getLinkProducerProperties());
        shufflerConsumer.subscribe(Collections.singletonList(config.getShufflerTopic()));
        shufflerService = new ShufflerService(config, shufflerConfig, shufflerConsumer, linkProducer, shuffleList, countDownLatch);
        shufflerServiceThread = new Thread(shufflerService);
        shufflerServiceThread.start();
    }

    @Override
    public void stopSchedule() {
        logger.info("Stop schedule service");
        shufflerService.close();
        shufflerServiceThread.interrupt();
        try {
            countDownLatch.await();
            logger.info("All service stopped");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(config.getShufflerProducerProperties())) {
                logger.info("Start sending {} messages from local message list to kafka", shuffleList.size());
                for (String link : shuffleList) {
                    producer.send(new ProducerRecord<>(config.getShufflerTopic(), link));
                }
                producer.flush();
            }
            logger.info("All messages sent to kafka");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
