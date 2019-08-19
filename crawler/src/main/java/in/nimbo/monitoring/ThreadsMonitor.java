package in.nimbo.monitoring;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;

import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

public class ThreadsMonitor implements Runnable {
    private ThreadPoolExecutor threadPoolExecutor;
    private ThreadGroup threadGroup;
    private Histogram waitingThreadsHistogram;
    private Histogram activeThreadsHistogram;

    public ThreadsMonitor(ThreadPoolExecutor threadPoolExecutor, ThreadGroup threadGroup) {
        this.threadPoolExecutor = threadPoolExecutor;
        this.threadGroup = threadGroup;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        waitingThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "waitingThreadsCounter"));
        activeThreadsHistogram = metricRegistry.histogram(MetricRegistry.name(ThreadsMonitor.class, "runningThreadsCounter"));
    }

    @Override
    public void run() {
        activeThreadsHistogram.update(threadPoolExecutor.getActiveCount());
        int waitingThreads = 0;
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread t : threadSet) {
            if (t.getThreadGroup().equals(threadGroup)) {
                Thread.State state = t.getState();
                if (state.equals(Thread.State.BLOCKED) || state.equals(Thread.State.TIMED_WAITING) || state.equals(Thread.State.WAITING)){
                    waitingThreads++;
                }
            }
        }
        waitingThreadsHistogram.update(waitingThreads);
    }
}
