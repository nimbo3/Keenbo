package in.nimbo.service.schedule;

import in.nimbo.conf.Config;
import in.nimbo.service.CrawlerServiceImpl;

public class ScheduleCrawling {
    private CrawlerServiceImpl crawlerServiceImpl;
    private Config config;

    public ScheduleCrawling(CrawlerServiceImpl crawlerServiceImpl, Config config) {
        this.crawlerServiceImpl = crawlerServiceImpl;
        this.config = config;
    }
}
