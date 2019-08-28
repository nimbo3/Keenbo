package in.nimbo.service;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Link;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.TimeoutException;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;

public class KafkaConsumerService {
    private BlockingQueue<Link> queue;
    private KafkaConfig config;
    private Consumer<String, Link> consumer;

    public KafkaConsumerService(BlockingQueue<Link> queue, KafkaConfig config, Consumer<String, Link> consumer) {
        this.queue = queue;
        this.config = config;
        this.consumer = consumer;
    }

    public void consume() throws IOException {
        try {
            while (true) {
                ConsumerRecords<String, Link> records = consumer.poll(Duration.ofMillis(config.getMaxPollDuration()));
                for (ConsumerRecord<String, Link> record : records) {
                    Link link = record.value();
                    queue.put(link);
                    try {
                        if (records.count() > 0) {
                            consumer.commitSync();
                        }
                    } catch (TimeoutException | CommitFailedException e) {
                    } catch (org.apache.kafka.common.errors.InterruptException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
