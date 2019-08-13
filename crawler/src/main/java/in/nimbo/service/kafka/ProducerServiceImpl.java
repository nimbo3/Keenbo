package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Anchor;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerServiceImpl implements ProducerService {
    private Logger logger = LoggerFactory.getLogger("app");
    private KafkaConfig config;
    private BlockingQueue<String> messageQueue;
    private Producer<String, String> linkProducer;
    private Producer<String, Page> pageProducer;
    private CrawlerService crawlerService;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private CountDownLatch countDownLatch;

    public ProducerServiceImpl(KafkaConfig config, BlockingQueue<String> messageQueue,
                               Producer<String, String> linkProducer, Producer<String, Page> pageProducer,
                               CrawlerService crawlerService, CountDownLatch countDownLatch) {
        this.config = config;
        this.messageQueue = messageQueue;
        this.linkProducer = linkProducer;
        this.pageProducer = pageProducer;
        this.crawlerService = crawlerService;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            while (!closed.get()) {
                String newLink = messageQueue.poll(100, TimeUnit.MILLISECONDS);
                if (newLink != null) {
                    handleLink(newLink);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (pageProducer != null)
                pageProducer.close();
            if (linkProducer != null)
                linkProducer.close();

            logger.info("Producers stopped");
            countDownLatch.countDown();
        }
    }

    private void handleLink(String link) {
        try {
            Optional<Page> optionalPage = crawlerService.crawl(link);
            if (optionalPage.isPresent()) {
                Page page = optionalPage.get();
                for (Anchor anchor : page.getAnchors()) {
                    linkProducer.send(new ProducerRecord<>(config.getLinkTopic(), anchor.getHref(), anchor.getHref()));
                }
                pageProducer.send(new ProducerRecord<>(config.getPageTopic(), page.getLink(), page));
            } else {
                linkProducer.send(new ProducerRecord<>(config.getLinkTopic(), link, link));
            }
        } catch (ParseLinkException | InvalidLinkException ignored) {
            // Ignore link
        }
    }
}
