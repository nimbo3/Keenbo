package in.nimbo.service.kafka;

import in.nimbo.config.KafkaConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ConsumerServiceTest {
    private static KafkaConfig kafkaConfig;
    private MockConsumer<String, String> kafkaConsumer;
    private MockProducer<String, String> kafkaProducer;
    private BlockingQueue<String> messageQueue;
    private ConsumerService consumerService;

    @BeforeClass
    public static void init() {
        kafkaConfig = KafkaConfig.load();
    }

    @Before
    public void beforeEachTest() {
        messageQueue = new LinkedBlockingQueue<>();
        kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        kafkaConsumer.subscribe(Collections.singletonList(kafkaConfig.getKafkaTopic()));
        consumerService = new ConsumerService(kafkaConsumer, messageQueue);
    }
}
