package in.nimbo.service.kafka;

import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerService implements Runnable {
    private Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private BlockingQueue<String> messageQueue;
    private Producer<String, String> producer;
    private String topic;
    private CrawlerService crawlerService;
    private AtomicBoolean closed;
    private CountDownLatch countDownLatch;

    public ProducerService(Producer<String, String> producer, String topic,
                           BlockingQueue<String> messageQueue, CrawlerService crawlerService, CountDownLatch countDownLatch) {
        this.producer = producer;
        this.messageQueue = messageQueue;
        this.topic = topic;
        this.crawlerService = crawlerService;
        this.countDownLatch = countDownLatch;
        closed = new AtomicBoolean(false);
    }

    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                String newLink = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (newLink != null) {
                    Set<String> crawl = crawlerService.crawl(newLink);
                    for (String link : crawl) {
                        producer.send(new ProducerRecord<>(topic, link, link));
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (producer != null)
                producer.close();
            logger.info("Producer service stopped");
            countDownLatch.countDown();
        }
    }
}
