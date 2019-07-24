package in.nimbo.service.kafka;

import in.nimbo.config.KafkaConfig;
import in.nimbo.exception.KafkaServiceException;
import in.nimbo.service.CrawlerService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;

public class KafkaService {
    private KafkaConfig kafkaConfig;
    private CrawlerService crawlerService;
    private KafkaConsumer<String, String> kafkaConsumer;

    public KafkaService(CrawlerService crawlerService, KafkaConfig kafkaConfig) {
        this.crawlerService = crawlerService;
        this.kafkaConfig = kafkaConfig;
    }

    /**
     * prepare kafka producer and consumer services and start threads to send/receive messages
     *
     * @throws KafkaServiceException if unable to prepare services
     */
    public void schedule() {
        ExecutorService executorService = Executors.newFixedThreadPool(kafkaConfig.getProducerCount() + 1);
        BlockingQueue<String> messageQueue = new LinkedTransferQueue<>();

        // Prepare consumer
        kafkaConsumer = new KafkaConsumer<>(kafkaConfig.getConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getKafkaTopic()));
        executorService.submit(new ConsumerService(kafkaConsumer, messageQueue));

        // Prepare producer
        for (int i = 0; i < kafkaConfig.getProducerCount(); i++) {
            KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getProducerProperties());
            executorService.submit(new ProducerService(producer, kafkaConfig.getKafkaTopic(), messageQueue, crawlerService));
        }
        executorService.shutdown();
    }

    /**
     * stop consumer service
     */
    public void stopSchedule() {
        kafkaConsumer.wakeup();
    }

    /**
     * send a message to kafka
     * @param message message value
     */
    public void sendMessage(String message) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaConfig.getProducerProperties());
        producer.send(new ProducerRecord<>(kafkaConfig.getKafkaTopic(), "ProducerService message", message));
        producer.flush();
    }
}
