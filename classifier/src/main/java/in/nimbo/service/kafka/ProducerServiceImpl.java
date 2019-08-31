package in.nimbo.service.kafka;

import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Link;
import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.common.utility.CloseUtility;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProducerServiceImpl implements ProducerService {
    private Logger logger = LoggerFactory.getLogger("classifier");
    private KafkaConfig kafkaConfig;
    private ClassifierConfig classifierConfig;
    private BlockingQueue<Link> messageQueue;
    private Producer<String, Link> linkProducer;
    private CrawlerService crawlerService;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private CountDownLatch countDownLatch;

    public ProducerServiceImpl(KafkaConfig kafkaConfig, ClassifierConfig classifierConfig,
                               CrawlerService crawlerService, BlockingQueue<Link> messageQueue,
                               Producer<String, Link> linkProducer, CountDownLatch countDownLatch) {
        this.kafkaConfig = kafkaConfig;
        this.classifierConfig = classifierConfig;
        this.crawlerService = crawlerService;
        this.messageQueue = messageQueue;
        this.linkProducer = linkProducer;
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
                Link link = messageQueue.take();
                if (link.getLevel() <= classifierConfig.getCrawlerLevel()) {
                    handle(link);
                }
            }
        } catch (InterruptedException e) {
            logger.info("Link Producer service interrupted successfully");
        } catch (Exception e) {
            logger.info("uncached exception in producer service");
        } finally {
            CloseUtility.closeSafely(linkProducer);
            logger.info("Link Producer service stopped successfully");
            countDownLatch.countDown();
        }
    }

    private void handle(Link link) {
        try {
            Optional<Page> crawl = crawlerService.crawl(link);
            if (crawl.isPresent()) {
                List<Link> crawledLinks = crawl.get().getAnchors().stream()
                        .map(anchor -> new Link(anchor.getHref(), link.getLabel(), link.getLevel() + 1))
                        .collect(Collectors.toList());
                for (Link crawlLink : crawledLinks) {
                    // TODO check domain *if needed*
                    linkProducer.send(new ProducerRecord<>(kafkaConfig.getTrainingTopic(), crawlLink));
                }
            } else {
                linkProducer.send(new ProducerRecord<>(kafkaConfig.getTrainingTopic(), link));
            }
        } catch (MalformedURLException | ParseLinkException | InvalidLinkException ignored) {
            logger.info("Skip corrupt link {}", link.getUrl());
        }
    }
}
