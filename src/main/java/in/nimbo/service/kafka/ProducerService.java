package in.nimbo.service.kafka;

import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerService implements Runnable {
    private Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private BlockingQueue<String> messageQueue;
    private Producer<String, String> producer;
    private String topic;
    private CrawlerService crawlerService;
    private AtomicBoolean closed;

    public ProducerService(Producer<String, String> producer, String topic,
                           BlockingQueue<String> messageQueue, CrawlerService crawlerService) {
        this.producer = producer;
        this.messageQueue = messageQueue;
        this.topic = topic;
        this.crawlerService = crawlerService;
        closed = new AtomicBoolean(false);
    }

    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                String newLink = messageQueue.take();
                List<String> crawl = crawlerService.crawl(newLink);
                for (String link : crawl) {
                    producer.send(new ProducerRecord<>(topic, "ProducerService message", link));
                }
            }
        } catch (InterruptedException e) {
            // ignore
        } finally {
            if (producer != null)
                producer.close();
            logger.info("Producer service stopped");
        }
    }
}
