package in.nimbo.service.schedule;

import in.nimbo.conf.Config;
import in.nimbo.service.CrawlerService;

public class ScheduleCrawling {
    private CrawlerService crawlerService;
    private Config config;

    public ScheduleCrawling(CrawlerService crawlerService, Config config) {
        this.crawlerService = crawlerService;
        this.config = config;
    }
}
