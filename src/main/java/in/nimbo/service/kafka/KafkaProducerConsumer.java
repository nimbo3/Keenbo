package in.nimbo.service.kafka;

import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class KafkaProducerConsumer implements Runnable {
    private Logger logger = LoggerFactory.getLogger(KafkaProducerConsumer.class);
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private CrawlerService crawlerService;

    public KafkaProducerConsumer(KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer,
                                 CrawlerService crawlerService) {
        this.producer = producer;
        this.consumer = consumer;
        this.crawlerService = crawlerService;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    String newLink = record.value();
                    List<String> crawl = crawlerService.crawl(newLink);
                    for (String link : crawl) {
                        producer.send(new ProducerRecord<>(KafkaService.KAFKA_TOPIC, "Producer message", link));
//                        logger.info("send " + link);
                    }
                }
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    logger.error("Unable to commit changes", e);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (producer != null)
                producer.close();
            if (consumer != null)
                consumer.close();
        }
    }
}
