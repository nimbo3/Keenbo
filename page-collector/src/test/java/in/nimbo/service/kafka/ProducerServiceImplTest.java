package in.nimbo.service.kafka;

import in.nimbo.TestUtility;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Meta;
import in.nimbo.common.entity.Page;
import in.nimbo.common.serializer.PageSerializer;
import in.nimbo.service.CollectorService;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class ProducerServiceImplTest {
    private MockProducer<String, Page> kafkaProducer;
    private BlockingQueue<Page> messageQueue;
    private ProducerService producerService;
    private CountDownLatch countDownLatch;
    private CollectorService collectorService;

    @Before
    public void beforeEachTest() {
        TestUtility.setMetricRegistry();
        messageQueue = new LinkedBlockingQueue<>();
        countDownLatch = new CountDownLatch(1);
        collectorService = mock(CollectorService.class);
        KafkaConfig kafkaConfig = KafkaConfig.load();
        kafkaProducer = new MockProducer<>(true, new StringSerializer(), new PageSerializer());
        producerService = new ProducerServiceImpl(kafkaConfig, messageQueue,
                kafkaProducer, collectorService, countDownLatch);
    }

    @Test
    public void producerTest() throws MalformedURLException, InterruptedException {
        Set<Anchor> anchors = new HashSet<>();
        anchors.add(new Anchor("https://stackoverflow.com", "stackoverflow"));
        anchors.add(new Anchor("https://google.com", "google"));
        Page page = new Page("http://nimbo.in", "nimbo", "sahab internship", anchors, new ArrayList<Meta>(), 1.0);
        messageQueue.add(page);
        when(collectorService.handle(page)).thenReturn(false);

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
        for (ProducerRecord<String, Page> record : kafkaProducer.history()) {
            Page returnedPage = record.value();
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
        assertEquals(0, countDownLatch.getCount());
    }
}
