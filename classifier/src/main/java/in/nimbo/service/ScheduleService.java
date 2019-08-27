package in.nimbo.service;

public class ScheduleService {
    private SampleExtractor sampleExtractor;

    public ScheduleService(SampleExtractor sampleExtractor) {
        this.sampleExtractor = sampleExtractor;
    }

    public void schedule() {
        for (int i = 0; i < 5; i++) {
            Thread thread = new Thread(() -> sampleExtractor.extract());
            thread.start();
        }
    }
}
