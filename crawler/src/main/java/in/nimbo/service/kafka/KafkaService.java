package in.nimbo.service.kafka;

public interface KafkaService {
    void schedule();

    void stopSchedule();

    void sendMessage(String message);
}
