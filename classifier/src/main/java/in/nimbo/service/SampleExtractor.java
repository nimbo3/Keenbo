package in.nimbo.service;

import in.nimbo.common.entity.Page;
import in.nimbo.common.exception.InvalidLinkException;
import in.nimbo.common.exception.ParseLinkException;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.config.ClassifierConfig;
import in.nimbo.entity.Link;
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

    public SampleExtractor(CrawlerService crawlerService, BlockingQueue<Link> queue, List<String> domains, ClassifierConfig config) {
        this.crawlerService = crawlerService;
        this.queue = queue;
        this.domains = domains;
        this.config = config;
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
                        List<Link> collect = crawl.get().getAnchors().stream().map(anchor -> new Link(anchor.getHref(), poll.getLabel(), poll.getLevel() + 1)).collect(Collectors.toList());
                        for (Link link : collect) {
                            try {
                                String domain = LinkUtility.getDomain(link.getUrl());
                                if (domains.contains(domain)) {
                                    queue.put(link);
                                }
                            } catch (InterruptedException e) {
                                logger.warn("Interrupted");
                            }
                        }
                    } else {
                        queue.put(poll);
                    }
                } catch (ParseLinkException | InvalidLinkException ignored) {
                    ignored.printStackTrace();
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
