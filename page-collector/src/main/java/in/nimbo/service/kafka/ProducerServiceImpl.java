package in.nimbo.service.kafka;

import edu.stanford.nlp.util.RuntimeInterruptedException;
import in.nimbo.common.config.KafkaConfig;
import in.nimbo.common.entity.Page;
import in.nimbo.common.utility.CloseUtility;
import in.nimbo.common.utility.LinkUtility;
import in.nimbo.service.CollectorService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProducerServiceImpl implements ProducerService {
    private CollectorService collectorService;
    private Logger logger = LoggerFactory.getLogger("collector");
    private KafkaConfig config;
    private BlockingQueue<Page> messageQueue;
    private Producer<String, Page> pageProducer;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private CountDownLatch countDownLatch;
    private List<Page> bufferList;

    public ProducerServiceImpl(KafkaConfig kafkaConfig, BlockingQueue<Page> messageQueue, List<Page> bufferList,
                               Producer<String, Page> pageProducer, CollectorService collectorService,
                               CountDownLatch countDownLatch) {
        this.config = kafkaConfig;
        this.messageQueue = messageQueue;
        this.pageProducer = pageProducer;
        this.countDownLatch = countDownLatch;
        this.collectorService = collectorService;
        this.bufferList = bufferList;
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
                Page page = messageQueue.take();
                try {
                    page.setLink(LinkUtility.normalize(page.getLink()));
                    bufferList.add(page);
                    int size = bufferList.size();
                    if (size > 0 && (size >= 2000 || retry >= 10)) {
                        handle();
                        retry = 0;
                    } else {
                        if (size == lastSize) {
                            retry++;
                        } else {
                            retry = 0;
                        }
                    }
                    lastSize = size;
                } catch (MalformedURLException e) {
                    logger.error("Illegal url format: {}", page.getLink(), e);
                }
            }
        } catch (InterruptedException | RuntimeInterruptedException e) {
            // ignored
        } finally {
            CloseUtility.closeSafely(pageProducer);
            logger.info("Page Producer service stopped successfully");
            countDownLatch.countDown();
        }
    }

    private void handle() {
        logger.info("Start collecting {} pages", bufferList.size());
        boolean collected = collectorService.processList(bufferList);
        if (!collected) {
            for (Page page : bufferList) {
                pageProducer.send(new ProducerRecord<>(config.getPageTopic(), page));
            }
        }
        logger.info("Finish collecting {} pages", bufferList.size());
        bufferList.clear();
    }
}
