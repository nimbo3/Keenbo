package in.nimbo.service.kafka;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.common.utility.CloseUtility;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerServiceImpl implements ProducerService {
    private Logger logger = LoggerFactory.getLogger("crawler");
    private KafkaConfig config;
    private BlockingQueue<String> messageQueue;
    private Producer<String, String> shufflerProducer;
    private Producer<String, Page> pageProducer;
    private CrawlerService crawlerService;

    private CountDownLatch countDownLatch;
    private AtomicBoolean closed = new AtomicBoolean(false);

    private Counter allLinksCounter;

    public ProducerServiceImpl(KafkaConfig config, BlockingQueue<String> messageQueue,
                               Producer<String, Page> pageProducer, Producer<String, String> shufflerProducer,
                               CrawlerService crawlerService, CountDownLatch countDownLatch) {
        this.config = config;
        this.messageQueue = messageQueue;
        this.pageProducer = pageProducer;
        this.shufflerProducer = shufflerProducer;
        this.crawlerService = crawlerService;
        this.countDownLatch = countDownLatch;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        allLinksCounter = metricRegistry.counter(MetricRegistry.name(ProducerService.class, "allLinksCounter"));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                String newLink = messageQueue.take();
                handleLink(newLink);
                allLinksCounter.inc();
            }
        } catch (InterruptedException | InterruptException e) {
            logger.info("Page Producer service interrupted successfully");
        } finally {
            CloseUtility.closeSafely(pageProducer);
            CloseUtility.closeSafely(shufflerProducer);
            logger.info("Page Producer service stopped successfully");
            countDownLatch.countDown();
        }
    }

    private void handleLink(String link) {
        try {
            Optional<Page> optionalPage = crawlerService.crawl(link);
            if (optionalPage.isPresent()) {
                Page page = optionalPage.get();
                for (Anchor anchor : page.getAnchors()) {
                    String anchorHref = anchor.getHref();
                    if (!anchorHref.contains("#")) {
                        shufflerProducer.send(new ProducerRecord<>(config.getShufflerTopic(), anchorHref));
                    }
                }
                pageProducer.send(new ProducerRecord<>(config.getPageTopic(), page));
            } else {
                shufflerProducer.send(new ProducerRecord<>(config.getShufflerTopic(), link));
            }
        } catch (ParseLinkException | InvalidLinkException ignored) {
            logger.info("Skip corrupt link {}", link);
        } catch (Exception e) {
            logger.error("Uncached exception", e);
        }
    }
}
