package in.nimbo.service.kafka;

import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class ProducerService implements Runnable {
    private Logger logger = LoggerFactory.getLogger(ProducerService.class);
    private BlockingQueue<String> messageQueue;
    private KafkaProducer<String, String> producer;
    private String topic;
    private CrawlerService crawlerService;

    public ProducerService(KafkaProducer<String, String> producer, String topic,
                           BlockingQueue<String> messageQueue, CrawlerService crawlerService) {
        this.producer = producer;
        this.messageQueue = messageQueue;
        this.topic = topic;
        this.crawlerService = crawlerService;
    }

    @Override
    public void run() {
        try {
            while (true) {
                String newLink = messageQueue.take();
                List<String> crawl = crawlerService.crawl(newLink);
                for (String link : crawl) {
                    producer.send(new ProducerRecord<>(topic, "ProducerService message", link));
//                    logger.info("send " + link);
                }
            }
        } catch (InterruptedException e) {
            // ignore
        } finally {
            if (producer != null)
                producer.close();
        }
    }
}
