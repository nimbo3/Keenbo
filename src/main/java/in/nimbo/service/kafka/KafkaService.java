package in.nimbo.service.kafka;

public interface KafkaService {
    void send(String topic, String message);

    String receive(String topic);
}
