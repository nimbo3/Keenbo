package in.nimbo.service.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;

public class ConsumerService implements Runnable {
    private Logger logger = LoggerFactory.getLogger(ConsumerService.class);
    private BlockingQueue<String> messageQueue;
    private Consumer<String, String> consumer;

    public ConsumerService(Consumer<String, String> consumer, BlockingQueue<String> messageQueue) {
        this.consumer = consumer;
        this.messageQueue = messageQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    messageQueue.put(record.value());
                }
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    logger.error("Unable to commit changes", e);
                }
            }
        } catch (WakeupException e) {
            logger.info("ConsumerService service stopped");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (consumer != null)
                consumer.close();
        }
    }
}
