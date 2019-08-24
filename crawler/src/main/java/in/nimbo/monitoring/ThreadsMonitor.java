package in.nimbo.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import java.util.List;

public class ThreadsMonitor implements Runnable {
    private List<Thread> threads;

    private Histogram newThreadsHistogram;
    private Histogram waitingThreadsHistogram;
    private Histogram activeThreadsHistogram;
    private Histogram terminatedThreadsHistogram;

    public ThreadsMonitor(List<Thread> threads) {
        this.threads = threads;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        waitingThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "waitingThreads"));
        activeThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "activeThreads"));
        terminatedThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "terminatedThreads"));
        newThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "newThreads"));
    }

    @Override
    public void run() {
        int newThreads = 0;
        int activeThreads = 0;
        int waitingThreads = 0;
        int terminatedThreads = 0;
        for (Thread thread : threads) {
            Thread.State state = thread.getState();
            if (state.equals(Thread.State.RUNNABLE)) {
                activeThreads++;
            } else if (state.equals(Thread.State.BLOCKED) || state.equals(Thread.State.TIMED_WAITING) || state.equals(Thread.State.WAITING)) {
                waitingThreads++;
            } else if (state.equals(Thread.State.TERMINATED)) {
                terminatedThreads++;
            } else if (state.equals(Thread.State.NEW)) {
                newThreads++;
            }
        }
        newThreadsHistogram.update(newThreads);
        activeThreadsHistogram.update(activeThreads);
        waitingThreadsHistogram.update(waitingThreads);
        terminatedThreadsHistogram.update(terminatedThreads);
    }
}
