package in.nimbo.service;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.utility.CloseUtility;
import in.nimbo.config.ShufflerConfig;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ShufflerService implements Runnable, Closeable {
    private Logger logger = LoggerFactory.getLogger("shuffler");
    private KafkaConfig kafkaConfig;
    private ShufflerConfig shufflerConfig;
    private List<String> shuffleList;
    private Consumer<String, String> shufflerConsumer;
    private Producer<String, String> linkProducer;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private CountDownLatch countDownLatch;
    private ThreadLocalRandom random = ThreadLocalRandom.current();

    private Timer shuffleLinksTimer;

    public ShufflerService(KafkaConfig kafkaConfig, ShufflerConfig shufflerConfig,
                           Consumer<String, String> shufflerConsumer, Producer<String, String> linkProducer,
                           List<String> shuffleList, CountDownLatch countDownLatch) {
        this.kafkaConfig = kafkaConfig;
        this.shufflerConfig = shufflerConfig;
        this.shufflerConsumer = shufflerConsumer;
        this.linkProducer = linkProducer;
        this.shuffleList = shuffleList;
        this.countDownLatch = countDownLatch;
        MetricRegistry metricRegistry = SharedMetricRegistries.getDefault();
        shuffleLinksTimer = metricRegistry.timer(MetricRegistry.name(ShufflerService.class, "shuffleLinksTimer"));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    @Override
    public void run() {
        try {
            int retry = 0;
            int lastSize = -1;
            while (!closed.get()) {
                ConsumerRecords<String, String> records = shufflerConsumer.poll(Duration.ofMillis(kafkaConfig.getMaxPollDuration()));
                for (ConsumerRecord<String, String> record : records) {
                    shuffleList.add(record.value());
                }
                int size = shuffleList.size();
                if (size > 0 && (size >= shufflerConfig.getShuffleSize() || retry >= 10)) {
                    processList();
                    retry = 0;
                    TimeUnit.MINUTES.sleep(shufflerConfig.getShuffleWaitMinutes());
                } else {
                    if (size == lastSize) {
                        retry++;
                    } else {
                        retry = 0;
                    }
                }
                lastSize = size;

                try {
                    if (records.count() > 0) {
                        shufflerConsumer.commitSync();
                    }
                } catch (TimeoutException | CommitFailedException e) {
                    logger.warn("Unable to commit changes for shuffle consumer");
                } catch (org.apache.kafka.common.errors.InterruptException e) {
                    logger.warn("Unable to commit changes for shuffle consumer because of interruption");
                    Thread.currentThread().interrupt();
                }
            }
        }catch (InterruptedException | InterruptException e) {
            logger.info("Shuffler service interrupted successfully");
        }  catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            CloseUtility.closeSafely(shufflerConsumer);
            CloseUtility.closeSafely(linkProducer);
            logger.info("Shuffler service stopped successfully");
            countDownLatch.countDown();
        }
    }

    private String[] shuffle(List<String> shuffleList) {
        String[] arr = shuffleList.toArray(new String[0]);
        for (int i = shuffleList.size(); i > 1; i--) {
            swap(arr, i - 1, random.nextInt(i));
        }
        return arr;
    }

    private static void swap(String[] arr, int i, int j) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private void processList() {
        logger.info("Start filtering {} links", shuffleList.size());
        List<String> filteredLinks = shuffleList;
        logger.info("Start shuffling {} links", filteredLinks.size());
        Timer.Context shuffleLinksTimerContext = shuffleLinksTimer.time();
        String[] shuffledLinks = shuffle(filteredLinks);
        shuffleLinksTimerContext.stop();
        logger.info("Finish shuffling {} links", filteredLinks.size());
        for (String link : shuffledLinks) {
            linkProducer.send(new ProducerRecord<>(kafkaConfig.getLinkTopic(), link));
        }
        logger.info("Added {} shuffled links to kafka", filteredLinks.size());
        shuffleList.clear();
    }
}
