package in.nimbo.service.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;

public class KafkaProducerConsumer implements Runnable {
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;

    public KafkaProducerConsumer(KafkaProducer<String, String> producer, KafkaConsumer<String, String> consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            producer.send(new ProducerRecord<>("links", "1", "my message 1"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value = %s",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                    String newId = Integer.toString(Integer.valueOf(record.key()) + 1);
                    producer.send(new ProducerRecord<>("links",
                            newId,
                            "my message " + newId));
                }
                consumer.commitSync();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (producer != null)
                producer.close();
            if (consumer != null)
                consumer.close();
        }
    }
}
