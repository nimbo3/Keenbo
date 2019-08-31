package in.nimbo.service.kafka;

import in.nimbo.TestUtility;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.*;
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
        TestUtility.setMetricRegistry();
        kafkaConfig = KafkaConfig.load();
    }

    @Test
    public void consumerRunTest() throws InterruptedException {
        BlockingQueue<Page> queue = new LinkedBlockingQueue<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        MockConsumer<String, Page> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getPageTopic()));
        ConsumerService consumerService = new ConsumerServiceImpl(kafkaConfig, kafkaConsumer, queue, countDownLatch);
        
        kafkaConsumer.rebalance(
                Collections.singleton(new TopicPartition(kafkaConfig.getPageTopic(), 0)));
        kafkaConsumer.seek(new TopicPartition(kafkaConfig.getPageTopic(), 0), 0);
        List<Page> crawledLinks = new ArrayList<>();
        Set<Anchor> nimboAnchors = new HashSet<>();
        nimboAnchors.add(new Anchor("http://google.com", "google"));
        nimboAnchors.add(new Anchor("http://stackoverflow.com", "stackoverflow"));
        Set<Anchor> sahabAnchors = new HashSet<>();
        sahabAnchors.add(new Anchor("http://google.com", "google"));
        sahabAnchors.add(new Anchor("http://stackoverflow.com", "stackoverflow"));
        sahabAnchors.add(new Anchor("http://nimbo.in", "nimbo"));
        try {
            crawledLinks.add(new Page("http://nimbo.in", "nimbo", "sahab internship", nimboAnchors, new ArrayList<>(), 1.2));
            crawledLinks.add(new Page("http://sahab.ir", "sahab", "sahab", sahabAnchors, new ArrayList<>(), 1.5));
        } catch (MalformedURLException e){
            Assert.fail();
        }
        for (int i = 0; i < crawledLinks.size(); i++) {
            kafkaConsumer.addRecord(new ConsumerRecord<>(
                    kafkaConfig.getPageTopic(), 0, i, "producer", crawledLinks.get(i)));
        }

        Thread consumerServiceThread = new Thread(consumerService);
        consumerServiceThread.start();
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                consumerService.close();
                consumerServiceThread.interrupt();
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
        consumerServiceThread.join();

        assertEquals(2, queue.size());
        for (Page page : crawledLinks) {
            Page returnedPage = queue.poll(1, TimeUnit.MILLISECONDS);
            assertNotNull(returnedPage);
            Assert.assertEquals(page.getLink(), returnedPage.getLink());
            Assert.assertEquals(page.getReversedLink(), returnedPage.getReversedLink());
            Assert.assertEquals(page.getLinkDepth(), returnedPage.getLinkDepth());
            Assert.assertEquals(page.getTitle(), returnedPage.getTitle());
            Assert.assertEquals(page.getContent(), returnedPage.getContent());
            Assert.assertEquals(page.getMetas(), returnedPage.getMetas());
            Assert.assertEquals(page.getAnchors(), returnedPage.getAnchors());
            Assert.assertEquals(String.valueOf(page.getRank()), String.valueOf(returnedPage.getRank()));
        }
        Assert.assertEquals(0, queue.size());
        Assert.assertEquals(0, countDownLatch.getCount());
    }
}
