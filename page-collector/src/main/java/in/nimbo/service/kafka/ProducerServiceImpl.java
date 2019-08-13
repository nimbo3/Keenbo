package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.service.CollectorService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerServiceImpl implements ProducerService {
    private CollectorService collectorService;
    private Logger logger = LoggerFactory.getLogger("app");
    private KafkaConfig config;
    private BlockingQueue<Page> messageQueue;
    private Producer<String, Page> pageProducer;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private CountDownLatch countDownLatch;

    public ProducerServiceImpl(KafkaConfig kafkaConfig, BlockingQueue<Page> messageQueue, Producer<String, Page> pageProducer,
                               CollectorService collectorService,
                               CountDownLatch countDownLatch) {
        this.config = kafkaConfig;
        this.messageQueue = messageQueue;
        this.pageProducer = pageProducer;
        this.countDownLatch = countDownLatch;
        this.collectorService = collectorService;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                Page page = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (page != null) {
                    handle(page);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            logger.info("Producer service stopped");
            countDownLatch.countDown();
        }
    }

    private void handle(Page page) {
        boolean collected = collectorService.handle(page);
        if (!collected) {
            pageProducer.send(new ProducerRecord<>(config.getPageTopic(), page.getLink(), page));
        }
    }
}
