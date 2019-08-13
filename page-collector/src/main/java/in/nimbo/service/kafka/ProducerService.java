package in.nimbo.service.kafka;

public interface ProducerService extends AutoCloseable, Runnable {
    @Override
    void close();
}
