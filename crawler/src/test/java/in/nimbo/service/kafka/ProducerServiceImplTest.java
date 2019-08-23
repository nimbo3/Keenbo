package in.nimbo.service.kafka;

import in.nimbo.TestUtility;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.serializer.PageSerializer;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ProducerServiceImplTest {
    private BlockingQueue<String> messageQueue;
    private ProducerService producerService;
    private CountDownLatch countDownLatch;
    private CrawlerService crawlerService;
    private MockProducer<String, String> shufflerProducer;
    private MockProducer<String, Page> pageProducer;

    @BeforeClass
    public static void init() {
        TestUtility.setMetricRegistry();
    }

    @Before
    public void beforeEachTest() {
        messageQueue = spy(new LinkedBlockingQueue<>());
        countDownLatch = new CountDownLatch(1);
        crawlerService = mock(CrawlerService.class);
        KafkaConfig kafkaConfig = KafkaConfig.load();
        shufflerProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        pageProducer = new MockProducer<>(true, new StringSerializer(), new PageSerializer());
        producerService = new ProducerServiceImpl(kafkaConfig, messageQueue,
                pageProducer, shufflerProducer,
                crawlerService, countDownLatch);
    }

    @Test
    public void producerTest() throws MalformedURLException, InterruptedException {
        Set<String> crawledLinks = new HashSet<>();
        crawledLinks.add("https://stackoverflow.com");
        crawledLinks.add("https://google.com");
        Set<Anchor> anchors = new HashSet<>();
        anchors.add(new Anchor("https://stackoverflow.com", "stackoverflow"));
        anchors.add(new Anchor("https://google.com", "google"));
        Page page = new Page("https://nimbo.in", "nimbo", "sahab internship", anchors, new ArrayList<>(), 1.0);
        when(crawlerService.crawl(anyString())).thenReturn(Optional.of(page));
        messageQueue.add("https://nimbo.in");


        Thread producerServiceThread = new Thread(producerService);
        producerServiceThread.start();
        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                producerService.close();
                producerServiceThread.interrupt();
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
        producerServiceThread.join();
        for (ProducerRecord<String, String> record : shufflerProducer.history()) {
            assertTrue(crawledLinks.contains(record.value()));
        }
        for (ProducerRecord<String, Page> record : pageProducer.history()) {
            assertEquals("https://nimbo.in", record.value().getLink());
        }
        assertEquals(0, countDownLatch.getCount());
    }
}
