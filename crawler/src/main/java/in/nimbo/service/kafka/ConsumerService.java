package in.nimbo.service.kafka;

public interface ConsumerService extends AutoCloseable, Runnable {
    @Override
    void close();
}
