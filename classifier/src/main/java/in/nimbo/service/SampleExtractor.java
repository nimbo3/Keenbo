package in.nimbo.service;

import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.common.entity.Link;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

public class SampleExtractor {
    private Logger logger = LoggerFactory.getLogger("crawler");
    private CrawlerService crawlerService;
    private BlockingQueue<Link> queue;
    private List<String> domains;
    private ClassifierConfig config;
    private KafkaProducerService producerService;

    public SampleExtractor(CrawlerService crawlerService, BlockingQueue<Link> queue, List<String> domains, ClassifierConfig config, KafkaProducerService producerService) {
        this.crawlerService = crawlerService;
        this.queue = queue;
        this.domains = domains;
        this.config = config;
        this.producerService = producerService;
    }

    public void extract() {
        try {
            while (true) {
                Link poll = queue.take();
                if (poll.getLevel() > config.getCrawlerLevel()) {
                    break;
                }
                try {
                    Optional<Page> crawl = crawlerService.crawl(poll);
                    if (crawl.isPresent()) {
                        List<Link> collect = crawl.get().getAnchors().stream()
                                .map(anchor -> new Link(anchor.getHref(), poll.getLabel(), poll.getLevel() + 1))
                                .collect(Collectors.toList());
                        for (Link link : collect) {
                            // TODO check domain *if needed*
                            producerService.produce(link);
                        }
                    } else {
                        producerService.produce(poll);
                    }
                } catch (ParseLinkException | InvalidLinkException ignored) {
                    logger.info("Skip corrupt link {}", poll.getUrl());
                } catch (Exception e) {
                    logger.error("Uncached exception", e);
                }
            }
        } catch (InterruptedException e) {
            logger.warn("Interrupted");
        }
    }
}
