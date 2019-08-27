package in.nimbo.service;

import in.nimbo.common.monitoring.ThreadsMonitor;
import in.nimbo.config.ClassifierConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduleService {
    private SampleExtractor sampleExtractor;
    private ClassifierConfig config;
    private List<Thread> threads;
    private ScheduledExecutorService threadMonitorService;

    public ScheduleService(SampleExtractor sampleExtractor, ClassifierConfig config) {
        this.sampleExtractor = sampleExtractor;
        this.config = config;
        this.threads = new ArrayList<>();
    }

    public List<Thread> getThreads() {
        return threads;
    }

    public void schedule() {
        for (int i = 0; i < config.getCrawlerThreads(); i++) {
            Thread thread = new Thread(() -> sampleExtractor.extract());
            thread.start();
            threads.add(thread);
        }
    }

    public void stop() {
        for (Thread thread : threads) {
            thread.interrupt();
        }
        threadMonitorService.shutdown();
    }

    private void startThreadsMonitoring() {
        ThreadsMonitor threadsMonitor = new ThreadsMonitor(threads);
        threadMonitorService = Executors.newScheduledThreadPool(1);
        threadMonitorService.scheduleAtFixedRate(threadsMonitor, 0, 1, TimeUnit.SECONDS);
    }
}
