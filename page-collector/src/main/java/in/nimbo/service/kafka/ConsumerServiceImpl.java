package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.utility.CloseUtility;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerServiceImpl implements ConsumerService {
    private Logger logger = LoggerFactory.getLogger("collector");
    private BlockingQueue<Page> messageQueue;
    private Consumer<String, Page> consumer;
    private CountDownLatch countDownLatch;
    private KafkaConfig kafkaConfig;

    private AtomicBoolean closed = new AtomicBoolean(false);

    public ConsumerServiceImpl(KafkaConfig kafkaConfig, Consumer<String, Page> consumer, BlockingQueue<Page> messageQueue,
                               CountDownLatch countDownLatch) {
        this.kafkaConfig = kafkaConfig;
        this.consumer = consumer;
        this.messageQueue = messageQueue;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                ConsumerRecords<String, Page> records = consumer.poll(Duration.ofMillis(kafkaConfig.getMaxPollDuration()));
                for (ConsumerRecord<String, Page> record : records) {
                    messageQueue.put(record.value());
                }

                try {
                    if (records.count() > 0) {
                        consumer.commitSync();
                    }
                } catch (TimeoutException | CommitFailedException e) {
                    logger.warn("Unable to commit changes for page consumer");
                } catch (org.apache.kafka.common.errors.InterruptException e) {
                    logger.warn("Unable to commit changes for page consumer because of interruption");
                    Thread.currentThread().interrupt();
                }
            }
        } catch (InterruptedException | InterruptException e) {
            logger.info("Consumer service interrupted successfully");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.info("Consumer service stopped with failures");
        } finally {
            CloseUtility.closeSafely(consumer);
            countDownLatch.countDown();
        }
    }
}
