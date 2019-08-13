package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.serializer.PageSerializer;
import in.nimbo.service.CollectorService;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

public class ProducerServiceImplTest {
    private MockProducer<String, Page> kafkaProducer;
    private BlockingQueue<Page> messageQueue;
    private ProducerService producerService;
    private CountDownLatch countDownLatch;
    private CollectorService collectorService;

    @Before
    public void beforeEachTest() {
        messageQueue = spy(new LinkedBlockingQueue<>());
        countDownLatch = new CountDownLatch(1);
        collectorService = mock(CollectorService.class);
        KafkaConfig kafkaConfig = KafkaConfig.load();
        kafkaProducer = new MockProducer<>(true, new StringSerializer(), new PageSerializer());
        producerService = new ProducerServiceImpl(kafkaConfig, messageQueue,
                kafkaProducer, collectorService, countDownLatch);
    }

    @Test
    public void producerTest() {
        Set<String> crawledLinks = new HashSet<>();
        crawledLinks.add("https://stackoverflow.com");
        crawledLinks.add("https://google.com");

        new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(2);
                producerService.close();
            } catch (InterruptedException e) {
                // ignored
            }
        }).start();
        producerService.run();
        for (ProducerRecord<String, Page> record : kafkaProducer.history()) {
            //          assertEquals(record.key(), record.value().getLink());
//            assertTrue(crawledLinks.contains(record.value()));
        }
        assertEquals(0, countDownLatch.getCount());
    }
}
