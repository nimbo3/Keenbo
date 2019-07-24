package in.nimbo.service.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerService implements Runnable {
    private Logger logger = LoggerFactory.getLogger(ConsumerService.class);
    private BlockingQueue<String> messageQueue;
    private Consumer<String, String> consumer;
    private AtomicBoolean closed;

    public ConsumerService(Consumer<String, String> consumer, BlockingQueue<String> messageQueue) {
        this.consumer = consumer;
        this.messageQueue = messageQueue;
        closed = new AtomicBoolean(false);
    }

    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    while (!closed.get()) {
                        messageQueue.offer(record.value(), 100, TimeUnit.MILLISECONDS);
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
            if (consumer != null)
                consumer.close();
            logger.info("Consumer service stopped");
        }
    }
}
