package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.utility.CloseUtility;
import in.nimbo.service.CollectorService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerServiceImpl implements ProducerService {
    private CollectorService collectorService;
    private Logger logger = LoggerFactory.getLogger("collector");
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
                Page page = messageQueue.take();
                handle(page);
            }
        } catch (InterruptedException | edu.stanford.nlp.util.RuntimeInterruptedException e) {
            // ignored
        } finally {
            CloseUtility.closeSafely(pageProducer);
            logger.info("Page Producer service stopped successfully");
            countDownLatch.countDown();
        }
    }

    private void handle(Page page) {
        boolean collected = collectorService.handle(page);
        if (!collected) {
            pageProducer.send(new ProducerRecord<>(config.getPageTopic(), page));
        }
    }
}
