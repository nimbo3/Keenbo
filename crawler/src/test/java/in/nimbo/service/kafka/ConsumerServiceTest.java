package in.nimbo.service.kafka;

import in.nimbo.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
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
import java.util.concurrent.TimeUnit;

public class ConsumerServiceTest {
    private static KafkaConfig kafkaConfig;
    private MockConsumer<String, String> kafkaConsumer;
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
        List<String> crawledLinks = new ArrayList<>();
        crawledLinks.add("https://stackoverflow.com");
        crawledLinks.add("https://google.com");
        for (int i = 0; i < crawledLinks.size(); i++) {
            kafkaConsumer.addRecord(new ConsumerRecord<>(
                    kafkaConfig.getKafkaTopic(), 0, i, "producer", crawledLinks.get(i)));
        }
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                consumerService.close();
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
        consumerService.run();
        for (String crawl : crawledLinks) {
            String link = messageQueue.take();
            Assert.assertEquals(link, crawl);
        }
        Assert.assertEquals(0, messageQueue.size());
        Assert.assertEquals(0, countDownLatch.getCount());
    }
}
