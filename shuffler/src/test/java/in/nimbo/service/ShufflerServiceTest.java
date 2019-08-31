package in.nimbo.service;

import in.nimbo.TestUtility;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.config.ShufflerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShufflerServiceTest {
    private static ShufflerService shufflerService;
    private static KafkaConfig kafkaConfig;
    private static MockConsumer<String, String> shufflerConsumer;
    private static MockProducer<String, String> linkProducer;
    private static List<String> shuffleList;
    private static CountDownLatch countDownLatch;

    @BeforeClass
    public static void init() {
        TestUtility.setMetricRegistry();
        kafkaConfig = KafkaConfig.load();
        ShufflerConfig shufflerConfig = ShufflerConfig.load();
        shufflerConfig.setShuffleSize(0);

        countDownLatch = new CountDownLatch(1);
        shuffleList = new ArrayList<>();
        shufflerConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        shufflerConsumer.subscribe(Collections.singletonList(kafkaConfig.getLinkTopic()));
        linkProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        shufflerService = new ShufflerService(kafkaConfig, shufflerConfig,
                shufflerConsumer, linkProducer, shuffleList, countDownLatch);
    }

    @Test
    public void shufflerTest() throws InterruptedException {
        shufflerConsumer.rebalance(
                Collections.singleton(new TopicPartition(kafkaConfig.getLinkTopic(), 0)));
        shufflerConsumer.seek(new TopicPartition(kafkaConfig.getLinkTopic(), 0), 0);
        List<String> crawledLinks = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            crawledLinks.add(String.valueOf(i));
        }
        for (int i = 0; i < crawledLinks.size(); i++) {
            shufflerConsumer.addRecord(new ConsumerRecord<>(
                    kafkaConfig.getLinkTopic(), 0, i, "producer", crawledLinks.get(i)));
        }

        Thread producerServiceThread = new Thread(shufflerService);
        producerServiceThread.start();
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                shufflerService.close();
                producerServiceThread.interrupt();
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
        producerServiceThread.join();
        assertTrue(shuffleList.isEmpty());
        for (ProducerRecord<String, String> record : linkProducer.history()) {
            assertTrue(crawledLinks.contains(record.value()));
        }
        assertEquals(0, countDownLatch.getCount());
    }
}
