package in.nimbo.service.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerService implements Runnable {
    private Logger logger = LoggerFactory.getLogger("app");
    private BlockingQueue<String> messageQueue;
    private Consumer<String, String> consumer;
    private AtomicBoolean closed;
    private CountDownLatch countDownLatch;

    public ConsumerService(Consumer<String, String> consumer, BlockingQueue<String> messageQueue,
                           CountDownLatch countDownLatch) {
        this.consumer = consumer;
        this.messageQueue = messageQueue;
        closed = new AtomicBoolean(false);
        this.countDownLatch = countDownLatch;
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
                    boolean isAdded = false;
                    while (!isAdded && !closed.get()) {
                        isAdded = messageQueue.offer(record.value(), 100, TimeUnit.MILLISECONDS);
                    }
                    if (closed.get()) {
                        break;
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (consumer != null)
                consumer.close();
            logger.info("Consumer service stopped");
            countDownLatch.countDown();
        }
    }
}
