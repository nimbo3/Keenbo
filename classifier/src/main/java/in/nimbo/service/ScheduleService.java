package in.nimbo.service;

import in.nimbo.common.monitoring.ThreadsMonitor;
import in.nimbo.config.ClassifierConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduleService {
    private SampleExtractor sampleExtractor;
    private KafkaConsumerService consumerService;
    private ClassifierConfig config;
    private List<Thread> threads;
    private ScheduledExecutorService threadMonitorService;

    public ScheduleService(SampleExtractor sampleExtractor, KafkaConsumerService consumerService, ClassifierConfig config) {
        this.sampleExtractor = sampleExtractor;
        this.consumerService = consumerService;
        this.config = config;
        this.threads = new ArrayList<>();
    }

    public void schedule() {
        for (int i = 0; i < config.getCrawlerThreads(); i++) {
            Thread thread = new Thread(() -> sampleExtractor.extract());
            thread.start();
            threads.add(thread);
        }
        Thread consumerThread = new Thread(() -> {
            try {
                consumerService.consume();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        threads.add(consumerThread);
        startThreadsMonitoring();
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
