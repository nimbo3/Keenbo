package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConsumerServiceImplTest {
    private static KafkaConfig kafkaConfig;

    @BeforeClass
    public static void init() {
        kafkaConfig = KafkaConfig.load();
    }

    @Test
    public void consumerRunTest() throws InterruptedException {
        BlockingQueue<Page> queue = new LinkedBlockingQueue<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        MockConsumer<String, Page> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getPageTopic()));
        ConsumerService consumerService = new ConsumerServiceImpl(kafkaConsumer, queue, countDownLatch);
        
        kafkaConsumer.rebalance(
                Collections.singleton(new TopicPartition(kafkaConfig.getPageTopic(), 0)));
        kafkaConsumer.seek(new TopicPartition(kafkaConfig.getPageTopic(), 0), 0);
        List<Page> crawledLinks = new ArrayList<>();
        for (int i = 0; i < crawledLinks.size(); i++) {
            kafkaConsumer.addRecord(new ConsumerRecord<>(
                    kafkaConfig.getPageTopic(), 0, i, "producer", crawledLinks.get(i)));
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
        //assertEquals(2, queue.size());
        for (Page page : crawledLinks) {
            Page page1 = queue.poll(1, TimeUnit.MILLISECONDS);
            assertNotNull(page1);
            Assert.assertEquals(page.getLink(), page1.getLink());// TODO
        }
        Assert.assertEquals(0, queue.size());
        Assert.assertEquals(0, countDownLatch.getCount());
    }
}
