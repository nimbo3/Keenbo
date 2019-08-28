package in.nimbo.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Link;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerService {
    private KafkaConfig config;
    private Producer<String, Link> producer;

    public KafkaProducerService(KafkaConfig config, Producer<String, Link> producer) {
        this.config = config;
        this.producer = producer;
    }

    public void produce(Link link) throws JsonProcessingException {
        producer.send(new ProducerRecord<>(config.getTrainingTopic(), link));
    }
}
