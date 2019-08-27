package in.nimbo.service;

import in.nimbo.config.ClassifierConfig;

public class ScheduleService {
    private SampleExtractor sampleExtractor;
    private ClassifierConfig config;

    public ScheduleService(SampleExtractor sampleExtractor, ClassifierConfig config) {
        this.sampleExtractor = sampleExtractor;
        this.config = config;
    }

    public void schedule() {
        for (int i = 0; i < config.getCrawlerThreads(); i++) {
            Thread thread = new Thread(() -> sampleExtractor.extract());
            thread.start();
        }
    }
}
