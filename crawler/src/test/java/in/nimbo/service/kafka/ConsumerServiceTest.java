package in.nimbo.service.kafka;

import in.nimbo.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerServiceTest {
    private static KafkaConfig kafkaConfig;
    private MockConsumer<String, String> kafkaConsumer;
    private MockProducer<String, String> kafkaProducer;
    private BlockingQueue<String> messageQueue;
    private ConsumerService consumerService;
    private CountDownLatch countDownLatch;

    @BeforeClass
    public static void init() {
        kafkaConfig = KafkaConfig.load();
    }

    @Before
    public void beforeEachTest() {
        messageQueue = new LinkedBlockingQueue<>();
        countDownLatch = new CountDownLatch(1);
        kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getKafkaTopic()));
        consumerService = new ConsumerService(kafkaConsumer, messageQueue, countDownLatch);
    }

    @Test
    public void consumerRunTest() throws InterruptedException {
        kafkaConsumer.rebalance(
                Collections.singleton(new TopicPartition(kafkaConfig.getKafkaTopic(), 0)));
        kafkaConsumer.seek(new TopicPartition(kafkaConfig.getKafkaTopic(), 0), 0);
        List<String> crawl = new ArrayList<>();
        crawl.add("https://stackoverflow.com");
        crawl.add("https://google.com");
        for (int i = 0; i < crawl.size(); i++) {
            kafkaConsumer.addRecord(new ConsumerRecord<>(
                    kafkaConfig.getKafkaTopic(), 0, i, "producer", crawl.get(i)));
        }
        new Thread(() -> {
            try {
                Thread.sleep(2000);
                consumerService.close();
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
        consumerService.run();
        for (int i = 0; i < crawl.size(); i++) {
            String link = messageQueue.take();
            Assert.assertEquals(link, crawl.get(i));
        }
        Assert.assertEquals(0, messageQueue.size());
        Assert.assertEquals(0, countDownLatch.getCount());
    }
}
