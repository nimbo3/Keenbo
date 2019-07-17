package in.nimbo.service.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class KafkaProducerConsumer implements Runnable {
    private Logger logger = LoggerFactory.getLogger(KafkaProducerConsumer.class);
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
                    System.out.println(String.format("Thread: %s, Topic: %s, Partition: %d, Offset: %d, Key: %s, Value = %s",
                            Thread.currentThread().getName(), record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                    String newId = Integer.toString(Integer.valueOf(record.key()) + 1);
                    producer.send(new ProducerRecord<>("links",
                            newId,
                            "my message " + newId));
                }
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    logger.error("Unable to commit changes", e);
                }
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
