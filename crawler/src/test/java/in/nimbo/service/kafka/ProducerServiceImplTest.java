package in.nimbo.service.kafka;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
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
    private MockProducer<String, String> linkProducer;
    private MockProducer<String, Page> pageProducer;

    @BeforeClass
    public static void init() {
        TestUtility.setMetric();
    }

    @Before
    public void beforeEachTest() {
        messageQueue = spy(new LinkedBlockingQueue<>());
        countDownLatch = new CountDownLatch(1);
        crawlerService = mock(CrawlerService.class);
        KafkaConfig kafkaConfig = KafkaConfig.load();
        linkProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        pageProducer = new MockProducer<>(true, new StringSerializer(), new PageSerializer());
        producerService = new ProducerServiceImpl(kafkaConfig, messageQueue,
                linkProducer, pageProducer,
                crawlerService, countDownLatch);
    }

    @Test
    public void producerTest() throws MalformedURLException {
        Set<String> crawledLinks = new HashSet<>();
        crawledLinks.add("https://stackoverflow.com");
        crawledLinks.add("https://google.com");
        Set<Anchor> anchors = new HashSet<>();
        anchors.add(new Anchor("https://stackoverflow.com", "stackoverflow"));
        anchors.add(new Anchor("https://google.com", "google"));
        Page page = new Page("https://nimbo.in", "nimbo", "sahab internship", anchors, new ArrayList<>(), 1.0);
        when(crawlerService.crawl(anyString())).thenReturn(Optional.of(page));
        messageQueue.add("https://nimbo.in");

        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                producerService.close();
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
        producerService.run();
        for (ProducerRecord<String, String> record : linkProducer.history()) {
            assertEquals(record.key(), record.value());
            assertTrue(crawledLinks.contains(record.value()));
        }
        assertEquals(0, countDownLatch.getCount());
    }
}
