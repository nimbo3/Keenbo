package in.nimbo.service.kafka;

import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class ProducerServiceTest {
    private MockProducer<String, String> kafkaProducer;
    private BlockingQueue<String> messageQueue;
    private ProducerService producerService;
    private CountDownLatch countDownLatch;
    private CrawlerService crawlerService;

    @Before
    public void beforeEachTest() {
        messageQueue = spy(new LinkedBlockingQueue<>());
        countDownLatch = new CountDownLatch(1);
        crawlerService = mock(CrawlerService.class);

        kafkaProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
        producerService = new ProducerService(kafkaProducer, "topic", messageQueue, crawlerService, countDownLatch);
    }

    @Test
    public void producerTest() {
        Set<String> crawledLinks = new HashSet<>();
        crawledLinks.add("https://stackoverflow.com");
        crawledLinks.add("https://google.com");
        when(crawlerService.crawl(anyString())).thenReturn(crawledLinks);
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
        for (ProducerRecord<String, String> record : kafkaProducer.history()) {
            assertEquals(record.key(), record.value());
            assertTrue(crawledLinks.contains(record.value()));
        }
        assertEquals(0, countDownLatch.getCount());
    }
}
